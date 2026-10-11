use super::mqttbytes::v5::{
    ConnAck, ConnectReturnCode, Disconnect, DisconnectReasonCode, Packet, PingReq, PubAck,
    PubAckReason, PubComp, PubCompReason, PubRec, PubRecReason, PubRel, PubRelReason, Publish,
    SubAck, Subscribe, SubscribeReasonCode, UnsubAck, UnsubAckReason, Unsubscribe,
};
use super::mqttbytes::{self, Error as MqttError, QoS};

use super::{Event, Incoming, Outgoing, Request};

use bytes::Bytes;
use fixedbitset::FixedBitSet;
use std::collections::{HashMap, VecDeque};
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    #[error("Conversion error {0:?}")]
    Coversion(#[from] core::num::TryFromIntError),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid: {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("A Subscribe packet must contain atleast one filter")]
    EmptySubscription,
    #[error("Mqtt serialization/deserialization error: {0}")]
    Deserialization(MqttError),
    #[error(
        "Cannot use topic alias '{alias:?}'. It's greater than the broker's maximum of '{max:?}'."
    )]
    InvalidAlias { alias: u16, max: u16 },
    #[error("Cannot send packet of size '{pkt_size:?}'. It's greater than the broker's maximum packet size of: '{max:?}'")]
    OutgoingPacketTooLarge { pkt_size: u32, max: u32 },
    #[error("Cannot receive packet of size '{pkt_size:?}'. It's greater than the client's maximum packet size of: '{max:?}'")]
    IncomingPacketTooLarge { pkt_size: usize, max: usize },
    #[error("Server sent disconnect with reason `{reason_string:?}` and code '{reason_code:?}' ")]
    ServerDisconnect {
        reason_code: DisconnectReasonCode,
        reason_string: Option<String>,
    },
    #[error("Connection failed with reason '{reason:?}' ")]
    ConnFail { reason: ConnectReturnCode },
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
}

impl From<mqttbytes::Error> for StateError {
    fn from(value: MqttError) -> Self {
        match value {
            MqttError::OutgoingPacketTooLarge { pkt_size, max } => {
                StateError::OutgoingPacketTooLarge { pkt_size, max }
            }
            e => StateError::Deserialization(e),
        }
    }
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
pub struct MqttState {
    /// Status of last ping
    pub await_pingresp: bool,
    /// Collision ping count. Collisions stop user requests
    /// which inturn trigger pings. Multiple pings without
    /// resolving collisions will result in error
    pub collision_ping_count: usize,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Number of outgoing packets holding a packet id until they are acked: QoS 1, 2 publishes,
    /// releases, subscribes and unsubscribes
    pub(crate) inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: FixedBitSet,
    /// Packet ids of subscribes and unsubscribes which aren't acked yet
    pub(crate) outgoing_sub_unsub: FixedBitSet,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: FixedBitSet,
    /// Last publish which collided on a packet id still in use
    pub collision: Option<Publish>,
    /// Buffered incoming packets
    pub events: VecDeque<Event>,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
    /// Map of alias_id->topic
    topic_alises: HashMap<u16, Bytes>,
    /// `topic_alias_maximum` RECEIVED via connack packet
    pub broker_topic_alias_max: u16,
    /// Maximum number of allowed inflight QoS1 & QoS2 requests
    pub(crate) max_outgoing_inflight: u16,
    /// Upper limit on the maximum number of allowed inflight QoS1 & QoS2 requests
    max_outgoing_inflight_upper_limit: u16,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16, manual_acks: bool) -> Self {
        MqttState {
            await_pingresp: false,
            collision_ping_count: 0,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            inflight: 0,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: FixedBitSet::with_capacity(max_inflight as usize + 1),
            outgoing_sub_unsub: FixedBitSet::with_capacity(max_inflight as usize + 1),
            incoming_pub: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            collision: None,
            // TODO: Optimize these sizes later
            events: VecDeque::with_capacity(100),
            manual_acks,
            topic_alises: HashMap::new(),
            // Set via CONNACK
            broker_topic_alias_max: 0,
            max_outgoing_inflight: max_inflight,
            max_outgoing_inflight_upper_limit: max_inflight,
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Request> {
        let mut pending = Vec::with_capacity(100);
        // remove and collect pending publishes
        for publish in self.outgoing_pub.iter_mut() {
            if let Some(publish) = publish.take() {
                let request = Request::Publish(publish);
                pending.push(request);
            }
        }

        // remove and collect pending releases
        for pkid in self.outgoing_rel.ones() {
            let request = Request::PubRel(PubRel::new(pkid as u16, None));
            pending.push(request);
        }
        self.outgoing_rel.clear();

        // a publish stashed on a collision was never sent, so it goes with the retransmissions
        if let Some(publish) = self.collision.take() {
            pending.push(Request::Publish(publish));
        }

        // subscribes and unsubscribes aren't retransmitted, so their packet ids are free
        self.outgoing_sub_unsub.clear();

        // remove packed ids of incoming qos2 publishes
        self.incoming_pub.clear();

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.inflight = 0;
        pending
    }

    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(
        &mut self,
        request: Request,
    ) -> Result<Option<Packet>, StateError> {
        let packet = match request {
            Request::Publish(publish) => self.outgoing_publish(publish)?,
            Request::PubRel(pubrel) => self.outgoing_pubrel(pubrel)?,
            Request::Subscribe(subscribe) => self.outgoing_subscribe(subscribe)?,
            Request::Unsubscribe(unsubscribe) => self.outgoing_unsubscribe(unsubscribe)?,
            Request::PingReq => self.outgoing_ping()?,
            Request::Disconnect => {
                self.outgoing_disconnect(DisconnectReasonCode::NormalDisconnection)?
            }
            Request::PubAck(puback) => self.outgoing_puback(puback)?,
            Request::PubRec(pubrec) => self.outgoing_pubrec(pubrec)?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        Ok(packet)
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(
        &mut self,
        mut packet: Incoming,
    ) -> Result<Option<Packet>, StateError> {
        self.events.push_back(Event::Incoming(packet.to_owned()));

        let outgoing = match &mut packet {
            Incoming::PingResp(_) => self.handle_incoming_pingresp()?,
            Incoming::Publish(publish) => self.handle_incoming_publish(publish)?,
            Incoming::SubAck(suback) => self.handle_incoming_suback(suback)?,
            Incoming::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback)?,
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback)?,
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec)?,
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel)?,
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp)?,
            Incoming::ConnAck(connack) => self.handle_incoming_connack(connack)?,
            Incoming::Disconnect(disconn) => self.handle_incoming_disconn(disconn)?,
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };

        self.last_incoming = Instant::now();
        Ok(outgoing)
    }

    pub fn handle_protocol_error(&mut self) -> Result<Option<Packet>, StateError> {
        // send DISCONNECT packet with REASON_CODE 0x82
        self.outgoing_disconnect(DisconnectReasonCode::ProtocolError)
    }

    fn handle_incoming_suback(
        &mut self,
        suback: &mut SubAck,
    ) -> Result<Option<Packet>, StateError> {
        for reason in suback.return_codes.iter() {
            match reason {
                SubscribeReasonCode::Success(qos) => {
                    debug!("SubAck Pkid = {:?}, QoS = {:?}", suback.pkid, qos);
                }
                _ => {
                    warn!("SubAck Pkid = {:?}, Reason = {:?}", suback.pkid, reason);
                }
            }
        }
        Ok(self.release_sub_unsub_pkid(suback.pkid))
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: &mut UnsubAck,
    ) -> Result<Option<Packet>, StateError> {
        for reason in unsuback.reasons.iter() {
            if reason != &UnsubAckReason::Success {
                warn!("UnsubAck Pkid = {:?}, Reason = {:?}", unsuback.pkid, reason);
            }
        }
        Ok(self.release_sub_unsub_pkid(unsuback.pkid))
    }

    fn handle_incoming_connack(
        &mut self,
        connack: &mut ConnAck,
    ) -> Result<Option<Packet>, StateError> {
        if connack.code != ConnectReturnCode::Success {
            return Err(StateError::ConnFail {
                reason: connack.code,
            });
        }

        if let Some(props) = &connack.properties {
            if let Some(topic_alias_max) = props.topic_alias_max {
                self.broker_topic_alias_max = topic_alias_max
            }

            if let Some(max_inflight) = props.receive_max {
                self.max_outgoing_inflight =
                    max_inflight.min(self.max_outgoing_inflight_upper_limit);
                // FIXME: Maybe resize the pubrec and pubrel queues here
                // to save some space.
            }
        }
        Ok(None)
    }

    fn handle_incoming_disconn(
        &mut self,
        disconn: &mut Disconnect,
    ) -> Result<Option<Packet>, StateError> {
        let reason_code = disconn.reason_code;
        let reason_string = if let Some(props) = &disconn.properties {
            props.reason_string.clone()
        } else {
            None
        };
        Err(StateError::ServerDisconnect {
            reason_code,
            reason_string,
        })
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(
        &mut self,
        publish: &mut Publish,
    ) -> Result<Option<Packet>, StateError> {
        let qos = publish.qos;

        let topic_alias = match &publish.properties {
            Some(props) => props.topic_alias,
            None => None,
        };

        if !publish.topic.is_empty() {
            if let Some(alias) = topic_alias {
                self.topic_alises.insert(alias, publish.topic.clone());
            }
        } else if let Some(alias) = topic_alias {
            if let Some(topic) = self.topic_alises.get(&alias) {
                topic.clone_into(&mut publish.topic);
            } else {
                self.handle_protocol_error()?;
            };
        }

        match qos {
            QoS::AtMostOnce => Ok(None),
            QoS::AtLeastOnce => {
                if !self.manual_acks {
                    let puback = PubAck::new(publish.pkid, None);
                    return self.outgoing_puback(puback);
                }
                Ok(None)
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                self.incoming_pub.insert(pkid as usize);

                if !self.manual_acks {
                    let pubrec = PubRec::new(pkid, None);
                    return self.outgoing_pubrec(pubrec);
                }
                Ok(None)
            }
        }
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<Option<Packet>, StateError> {
        let publish = self
            .outgoing_pub
            .get_mut(puback.pkid as usize)
            .ok_or(StateError::Unsolicited(puback.pkid))?;

        if publish.take().is_none() {
            error!("Unsolicited puback packet: {:?}", puback.pkid);
            return Err(StateError::Unsolicited(puback.pkid));
        }

        self.inflight -= 1;

        if puback.reason != PubAckReason::Success
            && puback.reason != PubAckReason::NoMatchingSubscribers
        {
            warn!(
                "PubAck Pkid = {:?}, reason: {:?}",
                puback.pkid, puback.reason
            );
        }

        Ok(self.send_collision(puback.pkid))
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
        let publish = self
            .outgoing_pub
            .get_mut(pubrec.pkid as usize)
            .ok_or(StateError::Unsolicited(pubrec.pkid))?;

        if publish.take().is_none() {
            error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
            return Err(StateError::Unsolicited(pubrec.pkid));
        }

        if pubrec.reason != PubRecReason::Success
            && pubrec.reason != PubRecReason::NoMatchingSubscribers
        {
            warn!(
                "PubRec Pkid = {:?}, reason: {:?}",
                pubrec.pkid, pubrec.reason
            );
            // a failed pubrec ends the exchange and frees the packet id
            self.inflight -= 1;
            return Ok(self.send_collision(pubrec.pkid));
        }

        // NOTE: Inflight - 1 for qos2 in comp
        self.outgoing_rel.insert(pubrec.pkid as usize);
        let event = Event::Outgoing(Outgoing::PubRel(pubrec.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(PubRel::new(pubrec.pkid, None))))
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
        if !self.incoming_pub.contains(pubrel.pkid as usize) {
            error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
            return Err(StateError::Unsolicited(pubrel.pkid));
        }
        self.incoming_pub.set(pubrel.pkid as usize, false);

        if pubrel.reason != PubRelReason::Success {
            warn!(
                "PubRel Pkid = {:?}, reason: {:?}",
                pubrel.pkid, pubrel.reason
            );
            return Ok(None);
        }

        let event = Event::Outgoing(Outgoing::PubComp(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubComp(PubComp::new(pubrel.pkid, None))))
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
        if !self.outgoing_rel.contains(pubcomp.pkid as usize) {
            error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }
        self.outgoing_rel.set(pubcomp.pkid as usize, false);
        self.inflight -= 1;

        if pubcomp.reason != PubCompReason::Success {
            warn!(
                "PubComp Pkid = {:?}, reason: {:?}",
                pubcomp.pkid, pubcomp.reason
            );
        }

        Ok(self.send_collision(pubcomp.pkid))
    }

    fn handle_incoming_pingresp(&mut self) -> Result<Option<Packet>, StateError> {
        self.await_pingresp = false;
        Ok(None)
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(&mut self, mut publish: Publish) -> Result<Option<Packet>, StateError> {
        if publish.qos != QoS::AtMostOnce {
            if publish.pkid == 0 {
                publish.pkid = self.next_pkid();
            }

            let pkid = publish.pkid;
            if self.outgoing_pub.get(pkid as usize).is_none() {
                return Err(StateError::Unsolicited(pkid));
            }

            // if another outgoing packet holds this pkid, the broker hasn't acked it yet. This is
            // possible only for a publish which already carries a pkid (a retransmission) or when
            // requests are handled while every pkid is in use
            if self.is_pkid_in_use(pkid) {
                info!("Collision on packet id = {:?}", publish.pkid);
                self.collision = Some(publish);
                let event = Event::Outgoing(Outgoing::AwaitAck(pkid));
                self.events.push_back(event);
                return Ok(None);
            }

            self.outgoing_pub[pkid as usize] = Some(publish.clone());
            self.inflight += 1;
        };

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            String::from_utf8(publish.topic.to_vec()).unwrap(),
            publish.pkid,
            publish.payload.len()
        );

        let pkid = publish.pkid;

        if let Some(props) = &publish.properties {
            if let Some(alias) = props.topic_alias {
                if alias > self.broker_topic_alias_max {
                    // We MUST NOT send a Topic Alias that is greater than the
                    // broker's Topic Alias Maximum.
                    return Err(StateError::InvalidAlias {
                        alias,
                        max: self.broker_topic_alias_max,
                    });
                }
            }
        };

        let event = Event::Outgoing(Outgoing::Publish(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Publish(publish)))
    }

    fn outgoing_pubrel(&mut self, pubrel: PubRel) -> Result<Option<Packet>, StateError> {
        let pubrel = self.save_pubrel(pubrel)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);

        let event = Event::Outgoing(Outgoing::PubRel(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(PubRel::new(pubrel.pkid, None))))
    }

    fn outgoing_puback(&mut self, puback: PubAck) -> Result<Option<Packet>, StateError> {
        let pkid = puback.pkid;
        let event = Event::Outgoing(Outgoing::PubAck(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubAck(puback)))
    }

    fn outgoing_pubrec(&mut self, pubrec: PubRec) -> Result<Option<Packet>, StateError> {
        let pkid = pubrec.pkid;
        let event = Event::Outgoing(Outgoing::PubRec(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRec(pubrec)))
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<Option<Packet>, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        if self.collision.is_some() {
            self.collision_ping_count += 1;
            if self.collision_ping_count >= 2 {
                return Err(StateError::CollisionTimeout);
            }
        }

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq, last incoming packet before {:?}, last outgoing request before {:?}",
            elapsed_in, elapsed_out,
        );

        let event = Event::Outgoing(Outgoing::PingReq);
        self.events.push_back(event);

        Ok(Some(Packet::PingReq(PingReq)))
    }

    fn outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
    ) -> Result<Option<Packet>, StateError> {
        if subscription.filters.is_empty() {
            return Err(StateError::EmptySubscription);
        }

        let pkid = self.take_sub_unsub_pkid()?;
        subscription.pkid = pkid;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.filters, subscription.pkid
        );

        let pkid = subscription.pkid;
        let event = Event::Outgoing(Outgoing::Subscribe(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Subscribe(subscription)))
    }

    fn outgoing_unsubscribe(
        &mut self,
        mut unsub: Unsubscribe,
    ) -> Result<Option<Packet>, StateError> {
        let pkid = self.take_sub_unsub_pkid()?;
        unsub.pkid = pkid;

        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.filters, unsub.pkid
        );

        let pkid = unsub.pkid;
        let event = Event::Outgoing(Outgoing::Unsubscribe(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Unsubscribe(unsub)))
    }

    fn outgoing_disconnect(
        &mut self,
        reason: DisconnectReasonCode,
    ) -> Result<Option<Packet>, StateError> {
        debug!("Disconnect with {:?}", reason);
        let event = Event::Outgoing(Outgoing::Disconnect);
        self.events.push_back(event);

        Ok(Some(Packet::Disconnect(Disconnect::new(reason))))
    }

    fn check_collision(&mut self, pkid: u16) -> Option<Publish> {
        if let Some(publish) = &self.collision {
            if publish.pkid == pkid {
                return self.collision.take();
            }
        }

        None
    }

    /// Sends the publish which collided on `pkid`, if any, now that `pkid` is free
    fn send_collision(&mut self, pkid: u16) -> Option<Packet> {
        let publish = self.check_collision(pkid)?;
        self.outgoing_pub[publish.pkid as usize] = Some(publish.clone());
        self.inflight += 1;

        let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
        self.events.push_back(event);
        self.collision_ping_count = 0;

        Some(Packet::Publish(publish))
    }

    /// Takes a free pkid for a subscribe or unsubscribe and holds it until the ack
    fn take_sub_unsub_pkid(&mut self) -> Result<u16, StateError> {
        let pkid = self.next_pkid();
        if self.is_pkid_in_use(pkid) {
            // Every pkid is in use. The eventloop takes no request needing a pkid while
            // `inflight` is at the maximum, so this is reached only when that limit is ignored
            error!("No free packet id for a subscribe or unsubscribe");
            return Err(StateError::InvalidState);
        }

        self.outgoing_sub_unsub.insert(pkid as usize);
        self.inflight += 1;
        Ok(pkid)
    }

    /// Frees the pkid of a subscribe or unsubscribe on its ack, and sends the publish which
    /// collided on it, if any
    fn release_sub_unsub_pkid(&mut self, pkid: u16) -> Option<Packet> {
        if !self.outgoing_sub_unsub.contains(pkid as usize) {
            warn!("Unsolicited suback/unsuback packet: {:?}", pkid);
            return None;
        }

        self.outgoing_sub_unsub.set(pkid as usize, false);
        self.inflight -= 1;
        self.send_collision(pkid)
    }

    /// A pkid is in use while an outgoing publish, release, subscribe or unsubscribe holding it
    /// waits for its ack
    fn is_pkid_in_use(&self, pkid: u16) -> bool {
        let pkid = pkid as usize;
        matches!(self.outgoing_pub.get(pkid), Some(Some(_)))
            || self.outgoing_rel.contains(pkid)
            || self.outgoing_sub_unsub.contains(pkid)
    }

    fn save_pubrel(&mut self, mut pubrel: PubRel) -> Result<PubRel, StateError> {
        let pubrel = match pubrel.pkid {
            // consider PacketIdentifier(0) as uninitialized packets
            0 => {
                pubrel.pkid = self.next_pkid();
                pubrel
            }
            _ => pubrel,
        };

        self.outgoing_rel.insert(pubrel.pkid as usize);
        self.inflight += 1;
        Ok(pubrel)
    }

    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    /// Packet ids cycle through 1 to the maximum set inflight messages, skipping those still in
    /// use (MQTT 5 section 2.2.1). When all of them are in use, which the eventloop prevents by
    /// taking no request while `inflight` is at the maximum, the next one in the cycle is
    /// returned and the caller detects the collision.
    fn next_pkid(&mut self) -> u16 {
        let max = self.max_outgoing_inflight.max(1);
        let following = |pkid: u16| if pkid >= max { 1 } else { pkid + 1 };

        // after a full cycle, `next_pkid` is back at the first candidate
        let mut next_pkid = following(self.last_pkid);
        for _ in 0..max {
            if !self.is_pkid_in_use(next_pkid) {
                break;
            }
            next_pkid = following(next_pkid);
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

#[cfg(test)]
mod test {
    use super::mqttbytes::v5::*;
    use super::mqttbytes::*;
    use super::{Event, Incoming, Outgoing, Request};
    use super::{MqttState, StateError};

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(u16::MAX, false)
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mut mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.next_pkid();

            // loops between 0-99. % 100 == 0 implies border
            let expected = i % 100;
            if expected == 0 {
                break;
            }

            assert_eq!(expected, pkid);
        }
    }

    #[test]
    fn outgoing_publish_should_set_pkid_and_add_publish_to_queue() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 3);
        assert_eq!(mqtt.inflight, 3);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 4);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn outgoing_publish_with_max_inflight_is_ok() {
        let mut mqtt = MqttState::new(2, false);

        // QoS2 publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id 2 is the limit
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // Every packet id is in use: this should cause a collision on the next one, 1
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 2);
        assert!(mqtt.collision.is_some());

        // The ack of 1 sends the collided publish with packet id 1
        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        // Now there should be space in the outgoing queue, at the free packet id 2
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        if let Event::Outgoing(Outgoing::PubAck(pkid)) = mqtt.events[0] {
            assert_eq!(pkid, 2);
        } else {
            panic!("missing puback");
        }

        if let Event::Outgoing(Outgoing::PubRec(pkid)) = mqtt.events[1] {
            assert_eq!(pkid, 3);
        } else {
            panic!("missing PubRec");
        }
    }

    #[test]
    fn incoming_publish_should_not_be_acked_with_manual_acks() {
        let mut mqtt = build_mqttstate();
        mqtt.manual_acks = true;

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        assert!(mqtt.incoming_pub.contains(3));
        assert!(mqtt.events.is_empty());
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish1).unwrap();
        mqtt.outgoing_publish(publish2).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub[1].is_none());
        assert!(mqtt.outgoing_pub[2].is_none());
    }

    #[test]
    fn incoming_puback_with_pkid_greater_than_max_inflight_should_be_handled_gracefully() {
        let mut mqtt = build_mqttstate();

        let got = mqtt
            .handle_incoming_puback(&PubAck::new(101, None))
            .unwrap_err();

        match got {
            StateError::Unsolicited(pkid) => assert_eq!(pkid, 101),
            e => panic!("Unexpected error: {}", e),
        }
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.outgoing_publish(publish1);
        let _publish_out = mqtt.outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(&PubRec::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub[1].clone();
        assert_eq!(backup.unwrap().pkid, 1);

        // check if the qos2 element's release pkid is 2
        assert!(mqtt.outgoing_rel.contains(2));
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        match mqtt.outgoing_publish(publish).unwrap().unwrap() {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrec(&PubRec::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrel(&PubRel::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1, None)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1, None))
            .unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1, None)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp(PingResp))
            .unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }

    fn build_subscribe() -> Subscribe {
        Subscribe::new(Filter::new("hello/world", QoS::AtLeastOnce), None)
    }

    fn build_unsubscribe() -> Unsubscribe {
        Unsubscribe::new("hello/world", None)
    }

    fn build_suback(pkid: u16) -> Incoming {
        Incoming::SubAck(SubAck {
            pkid,
            return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            properties: None,
        })
    }

    fn build_unsuback(pkid: u16) -> Incoming {
        Incoming::UnsubAck(UnsubAck {
            pkid,
            reasons: vec![UnsubAckReason::Success],
            properties: None,
        })
    }

    fn subscribe_pkid(packet: Option<Packet>) -> u16 {
        match packet {
            Some(Packet::Subscribe(subscribe)) => subscribe.pkid,
            packet => panic!("Expected a subscribe. Found = {:?}", packet),
        }
    }

    fn publish_pkid(packet: Option<Packet>) -> u16 {
        match packet {
            Some(Packet::Publish(publish)) => publish.pkid,
            packet => panic!("Expected a publish. Found = {:?}", packet),
        }
    }

    #[test]
    fn subscribe_does_not_reuse_the_pkid_of_an_outstanding_publish() {
        let mut mqtt = MqttState::new(2, false);

        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        assert_eq!(
            publish_pkid(mqtt.outgoing_publish(publish.clone()).unwrap()),
            1
        );
        assert_eq!(publish_pkid(mqtt.outgoing_publish(publish).unwrap()), 2);
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();

        // publish 1 still awaits its puback: 2 is the only free packet id
        let subscribe = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(subscribe), 2);
        assert_eq!(mqtt.inflight, 2);
    }

    #[test]
    fn unsubscribe_does_not_reuse_the_pkid_of_an_outstanding_publish() {
        let mut mqtt = MqttState::new(2, false);

        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.outgoing_publish(publish.clone()).unwrap();
        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();

        match mqtt.outgoing_unsubscribe(build_unsubscribe()).unwrap() {
            Some(Packet::Unsubscribe(unsubscribe)) => assert_eq!(unsubscribe.pkid, 2),
            packet => panic!("Expected an unsubscribe. Found = {:?}", packet),
        }
        assert_eq!(mqtt.inflight, 2);
    }

    #[test]
    fn publish_does_not_reuse_the_pkid_of_an_outstanding_subscribe() {
        let mut mqtt = MqttState::new(2, false);

        let subscribe = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(subscribe), 1);

        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        assert_eq!(
            publish_pkid(mqtt.outgoing_publish(publish.clone()).unwrap()),
            2
        );
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();

        // subscribe 1 still awaits its suback: 2 is the only free packet id
        assert_eq!(publish_pkid(mqtt.outgoing_publish(publish).unwrap()), 2);
        assert!(mqtt.collision.is_none());
    }

    #[test]
    fn publish_does_not_reuse_the_pkid_of_an_outstanding_pubrel() {
        let mut mqtt = MqttState::new(2, false);

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        assert_eq!(publish_pkid(mqtt.outgoing_publish(publish).unwrap()), 1);
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        assert_eq!(
            publish_pkid(mqtt.outgoing_publish(publish.clone()).unwrap()),
            2
        );
        mqtt.handle_incoming_pubrec(&PubRec::new(1, None)).unwrap();
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();

        // pubrel 1 still awaits its pubcomp: 2 is the only free packet id
        assert_eq!(publish_pkid(mqtt.outgoing_publish(publish).unwrap()), 2);
        assert!(mqtt.collision.is_none());
    }

    #[test]
    fn subscribes_count_toward_the_receive_maximum() {
        let mut mqtt = build_mqttstate();
        // as a connack with a receive maximum of 2 sets it
        mqtt.max_outgoing_inflight = 2;

        let first = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        let second = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(first), 1);
        assert_eq!(subscribe_pkid(second), 2);

        // the window is full, so the eventloop takes no new request...
        assert_eq!(mqtt.inflight(), 2);
        // ...and a subscribe pushed regardless is refused instead of reusing an outstanding id
        let events = mqtt.events.len();
        match mqtt.outgoing_subscribe(build_subscribe()) {
            Err(StateError::InvalidState) => (),
            v => panic!("Expected the subscribe to be refused. Found = {:?}", v),
        }
        assert_eq!(mqtt.events.len(), events);

        // a suback frees its packet id
        mqtt.handle_incoming_packet(build_suback(1)).unwrap();
        assert_eq!(mqtt.inflight(), 1);
        let third = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(third), 1);
        assert_eq!(mqtt.inflight(), 2);
    }

    #[test]
    fn suback_and_unsuback_free_their_pkids() {
        let mut mqtt = MqttState::new(2, false);

        mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        mqtt.outgoing_unsubscribe(build_unsubscribe()).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_packet(build_suback(1)).unwrap();
        assert_eq!(mqtt.inflight, 1);
        mqtt.handle_incoming_packet(build_unsuback(2)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        // an unsolicited acknowledgement frees nothing
        mqtt.handle_incoming_packet(build_suback(1)).unwrap();
        mqtt.handle_incoming_packet(build_unsuback(2)).unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn publish_colliding_with_a_subscribe_is_sent_on_its_suback() {
        let mut mqtt = MqttState::new(2, false);

        let subscribe = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(subscribe), 1);

        // a publish that already carries packet id 1, as a retransmission does
        let mut publish = build_outgoing_publish(QoS::AtLeastOnce);
        publish.pkid = 1;
        assert!(mqtt.outgoing_publish(publish).unwrap().is_none());
        assert!(mqtt.collision.is_some());

        let packet = mqtt.handle_incoming_packet(build_suback(1)).unwrap();
        assert_eq!(publish_pkid(packet), 1);
        assert!(mqtt.collision.is_none());
        assert!(mqtt.outgoing_pub[1].is_some());
        assert_eq!(mqtt.inflight, 1);
    }

    #[test]
    fn publish_sent_on_a_pubcomp_awaits_its_own_ack() {
        let mut mqtt = MqttState::new(1, false);

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1, None)).unwrap();

        // pubrel 1 holds the only packet id
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        assert!(mqtt.outgoing_publish(publish).unwrap().is_none());
        assert!(mqtt.collision.is_some());

        let packet = mqtt
            .handle_incoming_pubcomp(&PubComp::new(1, None))
            .unwrap();
        assert_eq!(publish_pkid(packet), 1);
        assert!(mqtt.outgoing_pub[1].is_some());
        assert_eq!(mqtt.inflight, 1);
        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn failed_acks_free_their_pkids() {
        let mut mqtt = MqttState::new(3, false);

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.outgoing_publish(publish.clone()).unwrap();
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // a pubrec with a failure reason ends the exchange
        let mut pubrec = PubRec::new(1, None);
        pubrec.reason = PubRecReason::UnspecifiedError;
        assert!(mqtt.handle_incoming_pubrec(&pubrec).unwrap().is_none());
        assert_eq!(mqtt.inflight, 1);

        // and so does a pubcomp with a failure reason
        mqtt.handle_incoming_pubrec(&PubRec::new(2, None)).unwrap();
        let mut pubcomp = PubComp::new(2, None);
        pubcomp.reason = PubCompReason::PacketIdentifierNotFound;
        assert!(mqtt.handle_incoming_pubcomp(&pubcomp).unwrap().is_none());
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn collision_is_resolved_by_a_failed_puback() {
        let mut mqtt = MqttState::new(1, false);

        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert!(mqtt.outgoing_publish(publish).unwrap().is_none());
        assert!(mqtt.collision.is_some());

        let mut puback = PubAck::new(1, None);
        puback.reason = PubAckReason::UnspecifiedError;
        let packet = mqtt.handle_incoming_puback(&puback).unwrap();
        assert_eq!(publish_pkid(packet), 1);
        assert!(mqtt.collision.is_none());
        assert_eq!(mqtt.inflight, 1);
    }

    #[test]
    fn clean_keeps_a_stashed_collision() {
        let mut mqtt = MqttState::new(2, false);

        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.outgoing_publish(publish.clone()).unwrap();
        mqtt.outgoing_publish(publish.clone()).unwrap();
        // every packet id is in use: the third publish is stashed
        assert!(mqtt.outgoing_publish(publish).unwrap().is_none());
        assert!(mqtt.collision.is_some());

        let pending = mqtt.clean();
        assert!(mqtt.collision.is_none());
        let publishes = pending
            .iter()
            .filter(|request| matches!(request, Request::Publish(_)))
            .count();
        assert_eq!(publishes, 3);
    }

    #[test]
    fn clean_frees_subscribe_and_unsubscribe_pkids() {
        let mut mqtt = MqttState::new(2, false);

        mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        mqtt.outgoing_unsubscribe(build_unsubscribe()).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // subscribes and unsubscribes aren't retransmitted
        assert!(mqtt.clean().is_empty());
        assert_eq!(mqtt.inflight, 0);
        let subscribe = mqtt.outgoing_subscribe(build_subscribe()).unwrap();
        assert_eq!(subscribe_pkid(subscribe), 1);
    }
}
