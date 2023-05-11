package buffer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/livekit/mediatransportutil"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

const (
	GapHistogramNumBins = 101
	NumSequenceNumbers  = 65536
	FirstSnapshotId     = 1
	SnInfoSize          = 8192
	SnInfoMask          = SnInfoSize - 1
	TooLargeOWDDelta    = 400 * time.Millisecond
)

type RTPFlowState struct {
	HasLoss            bool
	LossStartInclusive uint16
	LossEndExclusive   uint16
}

type IntervalStats struct {
	packets            uint32
	bytes              uint64
	headerBytes        uint64
	packetsPadding     uint32
	bytesPadding       uint64
	headerBytesPadding uint64
	packetsLost        uint32
	frames             uint32
}

type RTPDeltaInfo struct {
	StartTime            time.Time
	Duration             time.Duration
	Packets              uint32
	Bytes                uint64
	HeaderBytes          uint64
	PacketsDuplicate     uint32
	BytesDuplicate       uint64
	HeaderBytesDuplicate uint64
	PacketsPadding       uint32
	BytesPadding         uint64
	HeaderBytesPadding   uint64
	PacketsLost          uint32
	PacketsMissing       uint32
	Frames               uint32
	RttMax               uint32
	JitterMax            float64
	Nacks                uint32
	Plis                 uint32
	Firs                 uint32
}

type Snapshot struct {
	startTime             time.Time
	extStartSN            uint32
	extStartSNOverridden  uint32
	packetsDuplicate      uint32
	bytesDuplicate        uint64
	headerBytesDuplicate  uint64
	packetsLostOverridden uint32
	nacks                 uint32
	plis                  uint32
	firs                  uint32
	maxRtt                uint32
	maxJitter             float64
	maxJitterOverridden   float64
}

type SnInfo struct {
	hdrSize       uint16
	pktSize       uint16
	isPaddingOnly bool
	marker        bool
}

type RTCPSenderReportData struct {
	RTPTimestamp uint32
	NTPTimestamp mediatransportutil.NtpTime
	ArrivalTime  time.Time
}

type RTPStatsParams struct {
	ClockRate              uint32
	IsReceiverReportDriven bool
	Logger                 logger.Logger
}

type RTPStats struct {
	params RTPStatsParams
	logger logger.Logger

	lock sync.RWMutex

	initialized        bool
	resyncOnNextPacket bool

	startTime time.Time
	endTime   time.Time

	extStartSN uint32
	highestSN  uint16
	cycles     uint16

	extHighestSNOverridden uint32
	lastRRTime             time.Time
	lastRR                 rtcp.ReceptionReport

	extStartTS uint64
	highestTS  uint32
	tsCycles   uint32

	firstTime   time.Time
	highestTime time.Time

	lastTransit   uint32
	lastJitterRTP uint32

	bytes                uint64
	headerBytes          uint64
	bytesDuplicate       uint64
	headerBytesDuplicate uint64
	bytesPadding         uint64
	headerBytesPadding   uint64
	packetsDuplicate     uint32
	packetsPadding       uint32

	packetsOutOfOrder uint32

	packetsLost           uint32
	packetsLostOverridden uint32

	frames uint32

	jitter              float64
	maxJitter           float64
	jitterOverridden    float64
	maxJitterOverridden float64

	snInfos        [SnInfoSize]SnInfo
	snInfoWritePtr int

	gapHistogram [GapHistogramNumBins]uint32

	nacks        uint32
	nackAcks     uint32
	nackMisses   uint32
	nackRepeated uint32

	plis    uint32
	lastPli time.Time

	layerLockPlis    uint32
	lastLayerLockPli time.Time

	firs    uint32
	lastFir time.Time

	keyFrames    uint32
	lastKeyFrame time.Time

	rtt    uint32
	maxRtt uint32

	srData        *RTCPSenderReportData
	lastSRTime    time.Time
	lastSRNTP     mediatransportutil.NtpTime
	pidController *PIDController

	nextSnapshotId uint32
	snapshots      map[uint32]*Snapshot
}

func NewRTPStats(params RTPStatsParams) *RTPStats {
	r := &RTPStats{
		params:         params,
		logger:         params.Logger,
		nextSnapshotId: FirstSnapshotId,
		snapshots:      make(map[uint32]*Snapshot),
		pidController:  NewPIDController(),
	}

	r.pidController.SetGains(2.0, 0.5, 0.25)
	r.pidController.SetDerivativeLPF(0.02)
	r.pidController.SetOutputLimits(-0.025*float64(params.ClockRate), 0.025*float64(params.ClockRate))
	return r
}

func (r *RTPStats) Seed(from *RTPStats) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if from == nil || !from.initialized {
		return
	}

	r.initialized = from.initialized
	r.resyncOnNextPacket = from.resyncOnNextPacket

	r.startTime = from.startTime
	// do not clone endTime as a non-zero endTime indicates an ended object

	r.extStartSN = from.extStartSN
	r.highestSN = from.highestSN
	r.cycles = from.cycles

	r.extHighestSNOverridden = from.extHighestSNOverridden
	r.lastRRTime = from.lastRRTime
	r.lastRR = from.lastRR

	r.extStartTS = from.extStartTS
	r.highestTS = from.highestTS
	r.tsCycles = from.tsCycles

	r.firstTime = from.firstTime
	r.highestTime = from.highestTime

	r.lastTransit = from.lastTransit
	r.lastJitterRTP = from.lastJitterRTP

	r.bytes = from.bytes
	r.headerBytes = from.headerBytes
	r.bytesDuplicate = from.bytesDuplicate
	r.headerBytesDuplicate = from.headerBytesDuplicate
	r.bytesPadding = from.bytesPadding
	r.headerBytesPadding = from.headerBytesPadding
	r.packetsDuplicate = from.packetsDuplicate
	r.packetsPadding = from.packetsPadding

	r.packetsOutOfOrder = from.packetsOutOfOrder

	r.packetsLost = from.packetsLost
	r.packetsLostOverridden = from.packetsLostOverridden

	r.frames = from.frames

	r.jitter = from.jitter
	r.maxJitter = from.maxJitter
	r.jitterOverridden = from.jitterOverridden
	r.maxJitterOverridden = from.maxJitterOverridden

	r.snInfos = from.snInfos
	r.snInfoWritePtr = from.snInfoWritePtr

	r.gapHistogram = from.gapHistogram

	r.nacks = from.nacks
	r.nackAcks = from.nackAcks
	r.nackMisses = from.nackMisses
	r.nackRepeated = from.nackRepeated

	r.plis = from.plis
	r.lastPli = from.lastPli

	r.layerLockPlis = from.layerLockPlis
	r.lastLayerLockPli = from.lastLayerLockPli

	r.firs = from.firs
	r.lastFir = from.lastFir

	r.keyFrames = from.keyFrames
	r.lastKeyFrame = from.lastKeyFrame

	r.rtt = from.rtt
	r.maxRtt = from.maxRtt

	if from.srData != nil {
		srData := *from.srData
		r.srData = &srData
	} else {
		r.srData = nil
	}
	r.lastSRTime = from.lastSRTime
	r.lastSRNTP = from.lastSRNTP

	r.nextSnapshotId = from.nextSnapshotId
	for id, ss := range from.snapshots {
		ssCopy := *ss
		r.snapshots[id] = &ssCopy
	}
}

func (r *RTPStats) SetLogger(logger logger.Logger) {
	r.logger = logger
}

func (r *RTPStats) Stop() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.endTime = time.Now()
}

func (r *RTPStats) NewSnapshotId() uint32 {
	r.lock.Lock()
	defer r.lock.Unlock()

	id := r.nextSnapshotId
	if r.initialized {
		r.snapshots[id] = &Snapshot{
			startTime:            time.Now(),
			extStartSN:           r.extStartSN,
			extStartSNOverridden: r.extStartSN,
		}
	}

	r.nextSnapshotId++

	return id
}

func (r *RTPStats) IsActive() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.initialized && r.endTime.IsZero()
}

func (r *RTPStats) Update(rtph *rtp.Header, payloadSize int, paddingSize int, packetTime time.Time) (flowState RTPFlowState) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	first := false
	if !r.initialized {
		r.initialized = true

		r.startTime = time.Now()

		r.extStartSN = uint32(rtph.SequenceNumber)
		r.highestSN = rtph.SequenceNumber - 1
		r.cycles = 0

		r.extStartTS = uint64(rtph.Timestamp)
		r.highestTS = rtph.Timestamp
		r.tsCycles = 0

		r.firstTime = packetTime
		r.highestTime = packetTime

		first = true

		// initialize snapshots if any
		for i := uint32(FirstSnapshotId); i < r.nextSnapshotId; i++ {
			r.snapshots[i] = &Snapshot{
				startTime:            r.startTime,
				extStartSN:           r.extStartSN,
				extStartSNOverridden: r.extStartSN,
			}
		}
	}

	if r.resyncOnNextPacket {
		r.resyncOnNextPacket = false

		r.highestSN = rtph.SequenceNumber - 1
		r.highestTS = rtph.Timestamp
		r.highestTime = packetTime
	}

	hdrSize := uint64(rtph.MarshalSize())
	pktSize := hdrSize + uint64(payloadSize+paddingSize)
	isDuplicate := false
	diff := rtph.SequenceNumber - r.highestSN
	switch {
	// duplicate or out-of-order
	case diff == 0 || diff > (1<<15):
		if diff != 0 {
			r.packetsOutOfOrder++
		}

		// adjust start to account for out-of-order packets before a cycle completes
		if !r.maybeAdjustStartSN(rtph, pktSize, hdrSize, payloadSize) {
			if !r.isSnInfoLost(rtph.SequenceNumber) {
				r.bytesDuplicate += pktSize
				r.headerBytesDuplicate += hdrSize
				r.packetsDuplicate++
				isDuplicate = true
			} else {
				r.packetsLost--
				r.setSnInfo(rtph.SequenceNumber, uint16(pktSize), uint16(hdrSize), uint16(payloadSize), rtph.Marker)
			}
		}

	// in-order
	default:
		if diff > 1 {
			flowState.HasLoss = true
			flowState.LossStartInclusive = r.highestSN + 1
			flowState.LossEndExclusive = rtph.SequenceNumber
		}

		// update gap histogram
		r.updateGapHistogram(int(diff))

		// update missing sequence numbers
		r.clearSnInfos(r.highestSN+1, rtph.SequenceNumber)
		r.packetsLost += uint32(diff - 1)

		r.setSnInfo(rtph.SequenceNumber, uint16(pktSize), uint16(hdrSize), uint16(payloadSize), rtph.Marker)

		if rtph.SequenceNumber < r.highestSN && !first {
			r.cycles++
		}
		r.highestSN = rtph.SequenceNumber

		if rtph.Timestamp < r.highestTS && !first {
			r.tsCycles++
		}
		r.highestTS = rtph.Timestamp

		r.highestTime = packetTime
	}

	if !isDuplicate {
		if payloadSize == 0 {
			r.packetsPadding++
			r.bytesPadding += pktSize
			r.headerBytesPadding += hdrSize
		} else {
			r.bytes += pktSize
			r.headerBytes += hdrSize

			if rtph.Marker {
				r.frames++
			}

			r.updateJitter(rtph, packetTime)
		}
	}

	return
}

func (r *RTPStats) ResyncOnNextPacket() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.resyncOnNextPacket = true
}

func (r *RTPStats) maybeAdjustStartSN(rtph *rtp.Header, pktSize uint64, hdrSize uint64, payloadSize int) bool {
	if (r.getExtHighestSN() - r.extStartSN + 1) >= (NumSequenceNumbers / 2) {
		return false
	}

	if (rtph.SequenceNumber - uint16(r.extStartSN)) < (1 << 15) {
		return false
	}

	r.packetsLost += uint32(uint16(r.extStartSN)-rtph.SequenceNumber) - 1
	beforeAdjust := r.extStartSN
	r.extStartSN = uint32(rtph.SequenceNumber)

	r.setSnInfo(rtph.SequenceNumber, uint16(pktSize), uint16(hdrSize), uint16(payloadSize), rtph.Marker)

	for _, s := range r.snapshots {
		if s.extStartSN == beforeAdjust {
			s.extStartSN = r.extStartSN
		}
	}

	return true
}

func (r *RTPStats) GetTotalPacketsPrimary() uint32 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.getTotalPacketsPrimary()
}

func (r *RTPStats) getTotalPacketsPrimary() uint32 {
	packetsExpected := r.getExtHighestSN() - r.extStartSN + 1
	if r.packetsLost > packetsExpected {
		// should not happen
		return 0
	}

	packetsSeen := packetsExpected - r.packetsLost
	if r.packetsPadding > packetsSeen {
		return 0
	}

	return packetsSeen - r.packetsPadding
}

func (r *RTPStats) UpdateFromReceiverReport(rr rtcp.ReceptionReport) (rtt uint32, isRttChanged bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() || !r.params.IsReceiverReportDriven {
		return
	}

	rtt, err := mediatransportutil.GetRttMs(&rr, r.lastSRNTP, r.lastSRTime)
	if err == nil {
		isRttChanged = rtt != r.rtt
	} else {
		if err != mediatransportutil.ErrRttNoLastSenderReport {
			r.logger.Warnw("error getting rtt", err)
		}
	}

	if r.lastRRTime.IsZero() || r.extHighestSNOverridden <= rr.LastSequenceNumber {
		r.extHighestSNOverridden = rr.LastSequenceNumber
		r.packetsLostOverridden = rr.TotalLost

		if isRttChanged {
			r.rtt = rtt
			if rtt > r.maxRtt {
				r.maxRtt = rtt
			}
		}

		r.jitterOverridden = float64(rr.Jitter)
		if r.jitterOverridden > r.maxJitterOverridden {
			r.maxJitterOverridden = r.jitterOverridden
		}

		// update snapshots
		for _, s := range r.snapshots {
			if isRttChanged && rtt > s.maxRtt {
				s.maxRtt = rtt
			}

			if r.jitterOverridden > s.maxJitterOverridden {
				s.maxJitterOverridden = r.jitterOverridden
			}
		}

		r.lastRRTime = time.Now()
		r.lastRR = rr
	} else {
		// r.logger.Debugw(
		// 	fmt.Sprintf("receiver report potentially out of order, highestSN: existing: %d, received: %d", r.extHighestSNOverridden, rr.LastSequenceNumber),
		// 	"lastRRTime", r.lastRRTime,
		// 	"lastRR", r.lastRR,
		// 	"sinceLastRR", time.Since(r.lastRRTime),
		// 	"receivedRR", rr,
		// )
	}
	return
}

func (r *RTPStats) LastReceiverReport() time.Time {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.lastRRTime
}

func (r *RTPStats) UpdateNack(nackCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.nacks += nackCount
}

func (r *RTPStats) UpdateNackProcessed(nackAckCount uint32, nackMissCount uint32, nackRepeatedCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.nackAcks += nackAckCount
	r.nackMisses += nackMissCount
	r.nackRepeated += nackRepeatedCount
}

func (r *RTPStats) UpdatePliAndTime(pliCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.updatePliLocked(pliCount)
	r.updatePliTimeLocked()
}

func (r *RTPStats) UpdatePli(pliCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.updatePliLocked(pliCount)
}

func (r *RTPStats) updatePliLocked(pliCount uint32) {
	r.plis += pliCount
}

func (r *RTPStats) UpdatePliTime() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.updatePliTimeLocked()
}

func (r *RTPStats) updatePliTimeLocked() {
	r.lastPli = time.Now()
}

func (r *RTPStats) LastPli() time.Time {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.lastPli
}

func (r *RTPStats) TimeSinceLastPli() int64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return time.Now().UnixNano() - r.lastPli.UnixNano()
}

func (r *RTPStats) UpdateLayerLockPliAndTime(pliCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.layerLockPlis += pliCount
	r.lastLayerLockPli = time.Now()
}

func (r *RTPStats) UpdateFir(firCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.firs += firCount
}

func (r *RTPStats) UpdateFirTime() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.lastFir = time.Now()
}

func (r *RTPStats) UpdateKeyFrame(kfCount uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.keyFrames += kfCount
	r.lastKeyFrame = time.Now()
}

func (r *RTPStats) UpdateRtt(rtt uint32) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.endTime.IsZero() {
		return
	}

	r.rtt = rtt
	if rtt > r.maxRtt {
		r.maxRtt = rtt
	}

	for _, s := range r.snapshots {
		if rtt > s.maxRtt {
			s.maxRtt = rtt
		}
	}
}

func (r *RTPStats) GetRtt() uint32 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.rtt
}

func (r *RTPStats) SetRtcpSenderReportData(srData *RTCPSenderReportData) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if srData == nil {
		r.srData = nil
		return
	}

	// prevent against extreme case of anachronous sender reports
	if r.srData != nil && r.srData.NTPTimestamp > srData.NTPTimestamp {
		// r.logger.Debugw(
		// 	"received anachronous sender report",
		// 	"current", srData.NTPTimestamp.Time(),
		// 	"last", r.srData.NTPTimestamp.Time(),
		// )
		return
	}

	// TODO-REMOVE-AFTER-DEBUG-START
	// ntpTime := srData.NTPTimestamp.Time()

	// var ntpDiffSinceLast, arrivalDiffSinceLast time.Duration
	// var rtpDiffSinceLast uint32
	// if r.srData != nil {
	// 	ntpDiffSinceLast = ntpTime.Sub(r.srData.NTPTimestamp.Time())
	// 	rtpDiffSinceLast = srData.RTPTimestamp - r.srData.RTPTimestamp
	// 	arrivalDiffSinceLast = srData.ArrivalTime.Sub(r.srData.ArrivalTime)
	// }

	// timeSinceFirst := time.Since(r.firstTime) // ideally should use NTP time from SR, but that is a different time base, now is a resonable approximation
	// rtpDiffSinceFirst := getExtTS(srData.RTPTimestamp, r.tsCycles) - r.extStartTS
	// drift := int64(rtpDiffSinceFirst - uint64(timeSinceFirst.Nanoseconds()*int64(r.params.ClockRate)/1e9))
	// driftMs := (float64(drift) * 1000) / float64(r.params.ClockRate)

	// r.logger.Debugw(
	// 	"received sender report",
	// 	"ntp", ntpTime,
	// 	"rtp", srData.RTPTimestamp,
	// 	"arrival", srData.ArrivalTime,
	// 	"ntpDiff", ntpDiffSinceLast,
	// 	"rtpDiff", rtpDiffSinceLast,
	// 	"arrivalDiff", arrivalDiffSinceLast,
	// 	"expectedTimeDiff", float64(rtpDiffSinceLast)/float64(r.params.ClockRate),
	// 	"timeSinceFirst", timeSinceFirst,
	// 	"rtpDiffSinceFirst", rtpDiffSinceFirst,
	// 	"drift", drift,
	// 	"driftMs", driftMs,
	// 	"rate", float64(rtpDiffSinceFirst)/timeSinceFirst.Seconds(),
	// )
	// TODO-REMOVE-AFTER-DEBUG-END

	srDataCopy := *srData
	r.srData = &srDataCopy
}

func (r *RTPStats) GetRtcpSenderReportData() *RTCPSenderReportData {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.srData == nil {
		return nil
	}

	srDataCopy := *r.srData
	return &srDataCopy
}

func (r *RTPStats) GetExpectedRTPTimestamp(at time.Time) (uint32, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if !r.initialized {
		return 0, errors.New("uninitilaized")
	}

	timeDiff := at.Sub(r.firstTime)
	expectedRTPDiff := timeDiff.Nanoseconds() * int64(r.params.ClockRate) / 1e9
	expectedExtRTP := r.extStartTS + uint64(expectedRTPDiff)
	r.logger.Debugw(
		"expected RTP timestamp",
		"firstTime", r.firstTime.String(),
		"checkAt", at.String(),
		"timeDiff", timeDiff,
		"firstRTP", r.extStartTS,
		"expectedRTPDiff", expectedRTPDiff,
		"expectedExtRTP", expectedExtRTP,
		"expectedRTP", uint32(expectedExtRTP),
		"highestTS", r.highestTS,
		"highestTime", r.highestTime.String(),
	)
	return uint32(expectedExtRTP), nil
}

func (r *RTPStats) GetRtcpSenderReport(ssrc uint32) (*rtcp.SenderReport, float64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.initialized {
		return nil, 0.0
	}

	// construct current time based on monotonic clock
	timeSinceFirst := time.Since(r.firstTime)
	now := r.firstTime.Add(timeSinceFirst)
	nowNTP := mediatransportutil.ToNtpTime(now)

	expectedExtRTP := r.extStartTS + uint64(timeSinceFirst.Nanoseconds()*int64(r.params.ClockRate)/1e9)
	if getExtTS(r.highestTS, r.tsCycles) > expectedExtRTP || now.Before(r.highestTime) {
		// r.logger.Debugw(
		// 	"sending anachronous sender report",
		// 	"firstTime", r.firstTime.String(),
		// 	"currentTime", now.String(),
		// 	"highestTime", r.highestTime.String(),
		// 	"timeSinceFirst", timeSinceFirst,
		// 	"extStartTS", r.extStartTS,
		// 	"highestExtRTP", getExtTS(r.highestTS, r.tsCycles),
		// 	"expectedExtRTP", expectedExtRTP,
		// )
	}

	timeSinceHighest := time.Since(r.highestTime)
	nowRTP := r.highestTS + uint32(timeSinceHighest.Nanoseconds()*int64(r.params.ClockRate)/1e9)

	// TODO-REMOVE-AFTER-DEBUG-START
	timeSinceFirst = nowNTP.Time().Sub(r.firstTime)
	rtpDiffSinceFirst := getExtTS(nowRTP, r.tsCycles) - r.extStartTS
	measurement := float64(rtpDiffSinceFirst) / timeSinceFirst.Seconds()
	pidOutput := r.pidController.Update(
		float64(r.params.ClockRate),
		measurement,
		now,
	)
	// r.logger.Debugw(
	// 	"pid controller output",
	// 	"measurement", measurement,
	// 	"errorTerm", float64(r.params.ClockRate)-measurement,
	// 	"pidOutput", pidOutput,
	// )
	// TODO-REMOVE-AFTER-DEBUG-STOP

	// TODO-REMOVE-AFTER-DEBUG-START
	// ntpTime := nowNTP.Time()

	// ntpDiffLocal := ntpTime.Sub(r.highestTime)
	// rtpDiffLocal := int32(nowRTP - r.highestTS)
	// rtpOffsetLocal := int32(nowRTP - r.highestTS - uint32(ntpDiffLocal.Nanoseconds()*int64(r.params.ClockRate)/1e9))

	// rtpDiffSinceFirst := getExtTS(nowRTP, r.tsCycles) - r.extStartTS
	// drift := int64(rtpDiffSinceFirst - uint64(timeSinceFirst.Nanoseconds()*int64(r.params.ClockRate)/1e9))
	// driftMs := (float64(drift) * 1000) / float64(r.params.ClockRate)
	// r.logger.Debugw(
	// 	"sending sender report",
	// 	"highestTS", r.highestTS,
	// 	"highestTime", r.highestTime.String(),
	// 	"reportTS", nowRTP,
	// 	"expectedTS", uint32(expectedExtRTP),
	// 	"reportTime", ntpTime.String(),
	// 	"rtpDiffLocal", rtpDiffLocal,
	// 	"ntpDiffLocal", ntpDiffLocal,
	// 	"rtpOffsetLocal", rtpOffsetLocal,
	// 	"timeSinceFirst", timeSinceFirst,
	// 	"rtpDiffSinceFirst", rtpDiffSinceFirst,
	// 	"drift", drift,
	// 	"driftMs", driftMs,
	// 	"rate", float64(rtpDiffSinceFirst)/timeSinceFirst.Seconds(),
	// )
	// TODO-REMOVE-AFTER-DEBUG-END

	r.lastSRTime = now
	r.lastSRNTP = nowNTP

	return &rtcp.SenderReport{
		SSRC:        ssrc,
		NTPTime:     uint64(nowNTP),
		RTPTime:     nowRTP,
		PacketCount: r.getTotalPacketsPrimary() + r.packetsDuplicate + r.packetsPadding,
		OctetCount:  uint32(r.bytes + r.bytesDuplicate + r.bytesPadding),
	}, pidOutput
}

func (r *RTPStats) SnapshotRtcpReceptionReport(ssrc uint32, proxyFracLost uint8, snapshotId uint32) *rtcp.ReceptionReport {
	r.lock.Lock()
	then, now := r.getAndResetSnapshot(snapshotId, false)
	r.lock.Unlock()

	if now == nil || then == nil {
		return nil
	}

	r.lock.RLock()
	defer r.lock.RUnlock()

	packetsExpected := now.extStartSN - then.extStartSN
	if packetsExpected > NumSequenceNumbers {
		r.logger.Warnw(
			"too many packets expected in receiver report",
			fmt.Errorf("start: %d, end: %d, expected: %d", then.extStartSN, now.extStartSN, packetsExpected),
		)
		return nil
	}
	if packetsExpected == 0 {
		return nil
	}

	intervalStats := r.getIntervalStats(uint16(then.extStartSN), uint16(now.extStartSN))
	packetsLost := intervalStats.packetsLost
	lossRate := float32(packetsLost) / float32(packetsExpected)
	fracLost := uint8(lossRate * 256.0)
	if proxyFracLost > fracLost {
		fracLost = proxyFracLost
	}

	var dlsr uint32
	if r.srData != nil && !r.srData.ArrivalTime.IsZero() {
		delayMS := uint32(time.Since(r.srData.ArrivalTime).Milliseconds())
		dlsr = (delayMS / 1e3) << 16
		dlsr |= (delayMS % 1e3) * 65536 / 1000
	}

	lastSR := uint32(0)
	if r.srData != nil {
		lastSR = uint32(r.srData.NTPTimestamp >> 16)
	}
	return &rtcp.ReceptionReport{
		SSRC:               ssrc,
		FractionLost:       fracLost,
		TotalLost:          r.packetsLost,
		LastSequenceNumber: now.extStartSN,
		Jitter:             uint32(r.jitter),
		LastSenderReport:   lastSR,
		Delay:              dlsr,
	}
}

func (r *RTPStats) DeltaInfo(snapshotId uint32) *RTPDeltaInfo {
	r.lock.Lock()
	then, now := r.getAndResetSnapshot(snapshotId, false)
	r.lock.Unlock()

	if now == nil || then == nil {
		return nil
	}

	r.lock.RLock()
	defer r.lock.RUnlock()

	startTime := then.startTime
	endTime := now.startTime

	packetsExpected := now.extStartSN - then.extStartSN
	if packetsExpected > NumSequenceNumbers {
		r.logger.Warnw(
			"too many packets expected in delta",
			fmt.Errorf("start: %d, end: %d, expected: %d", then.extStartSN, now.extStartSN, packetsExpected),
		)
		return nil
	}
	if packetsExpected == 0 {
		return &RTPDeltaInfo{
			StartTime: startTime,
			Duration:  endTime.Sub(startTime),
		}
	}

	intervalStats := r.getIntervalStats(uint16(then.extStartSN), uint16(now.extStartSN))
	return &RTPDeltaInfo{
		StartTime:            startTime,
		Duration:             endTime.Sub(startTime),
		Packets:              packetsExpected - intervalStats.packetsPadding,
		Bytes:                intervalStats.bytes,
		HeaderBytes:          intervalStats.headerBytes,
		PacketsDuplicate:     now.packetsDuplicate - then.packetsDuplicate,
		BytesDuplicate:       now.bytesDuplicate - then.bytesDuplicate,
		HeaderBytesDuplicate: now.headerBytesDuplicate - then.headerBytesDuplicate,
		PacketsPadding:       intervalStats.packetsPadding,
		BytesPadding:         intervalStats.bytesPadding,
		HeaderBytesPadding:   intervalStats.headerBytesPadding,
		PacketsLost:          intervalStats.packetsLost,
		Frames:               intervalStats.frames,
		RttMax:               then.maxRtt,
		JitterMax:            then.maxJitter / float64(r.params.ClockRate) * 1e6,
		Nacks:                now.nacks - then.nacks,
		Plis:                 now.plis - then.plis,
		Firs:                 now.firs - then.firs,
	}
}

func (r *RTPStats) DeltaInfoOverridden(snapshotId uint32) *RTPDeltaInfo {
	if !r.params.IsReceiverReportDriven {
		return nil
	}

	r.lock.Lock()
	then, now := r.getAndResetSnapshot(snapshotId, true)
	r.lock.Unlock()

	if now == nil || then == nil {
		return nil
	}

	r.lock.RLock()
	defer r.lock.RUnlock()

	startTime := then.startTime
	endTime := now.startTime

	packetsExpected := now.extStartSNOverridden - then.extStartSNOverridden
	if packetsExpected > NumSequenceNumbers {
		r.logger.Warnw(
			"too many packets expected in delta",
			fmt.Errorf("start: %d, end: %d, expected: %d", then.extStartSNOverridden, now.extStartSNOverridden, packetsExpected),
		)
		return nil
	}
	if packetsExpected == 0 {
		// not received RTCP RR (OR) publisher is not producing any data
		return nil
	}

	intervalStats := r.getIntervalStats(uint16(then.extStartSNOverridden), uint16(now.extStartSNOverridden))
	packetsMissing := intervalStats.packetsLost
	packetsLost := now.packetsLostOverridden - then.packetsLostOverridden
	if int32(packetsLost) < 0 {
		packetsLost = 0
	}

	if packetsLost > packetsExpected {
		r.logger.Warnw(
			"unexpected number of packets lost",
			fmt.Errorf(
				"start: %d, end: %d, expected: %d, lost: report: %d, interval: %d",
				then.extStartSNOverridden,
				now.extStartSNOverridden,
				packetsExpected,
				now.packetsLostOverridden-then.packetsLostOverridden,
				intervalStats.packetsLost,
			),
		)
		packetsLost = packetsExpected
	}

	// discount jitter from publisher side + internal processing
	maxJitter := then.maxJitterOverridden - then.maxJitter
	if maxJitter < 0.0 {
		maxJitter = 0.0
	}
	maxJitterTime := maxJitter / float64(r.params.ClockRate) * 1e6

	return &RTPDeltaInfo{
		StartTime:            startTime,
		Duration:             endTime.Sub(startTime),
		Packets:              packetsExpected - intervalStats.packetsPadding,
		Bytes:                intervalStats.bytes,
		HeaderBytes:          intervalStats.headerBytes,
		PacketsDuplicate:     now.packetsDuplicate - then.packetsDuplicate,
		BytesDuplicate:       now.bytesDuplicate - then.bytesDuplicate,
		HeaderBytesDuplicate: now.headerBytesDuplicate - then.headerBytesDuplicate,
		PacketsPadding:       intervalStats.packetsPadding,
		BytesPadding:         intervalStats.bytesPadding,
		HeaderBytesPadding:   intervalStats.headerBytesPadding,
		PacketsLost:          packetsLost,
		PacketsMissing:       packetsMissing,
		Frames:               intervalStats.frames,
		RttMax:               then.maxRtt,
		JitterMax:            maxJitterTime,
		Nacks:                now.nacks - then.nacks,
		Plis:                 now.plis - then.plis,
		Firs:                 now.firs - then.firs,
	}
}

func (r *RTPStats) ToString() string {
	p := r.ToProto()
	if p == nil {
		return ""
	}

	r.lock.RLock()
	defer r.lock.RUnlock()

	expectedPackets := r.getExtHighestSN() - r.extStartSN + 1
	expectedPacketRate := float64(expectedPackets) / p.Duration

	str := fmt.Sprintf("t: %+v|%+v|%.2fs", p.StartTime.AsTime().Format(time.UnixDate), p.EndTime.AsTime().Format(time.UnixDate), p.Duration)

	str += fmt.Sprintf(" sn: %d|%d", r.extStartSN, r.getExtHighestSN())
	str += fmt.Sprintf(", ep: %d|%.2f/s", expectedPackets, expectedPacketRate)

	str += fmt.Sprintf(", p: %d|%.2f/s", p.Packets, p.PacketRate)
	str += fmt.Sprintf(", l: %d|%.1f/s|%.2f%%", p.PacketsLost, p.PacketLossRate, p.PacketLossPercentage)
	str += fmt.Sprintf(", b: %d|%.1fbps|%d", p.Bytes, p.Bitrate, p.HeaderBytes)
	str += fmt.Sprintf(", f: %d|%.1f/s / %d|%+v", p.Frames, p.FrameRate, p.KeyFrames, p.LastKeyFrame.AsTime().Format(time.UnixDate))

	str += fmt.Sprintf(", d: %d|%.2f/s", p.PacketsDuplicate, p.PacketDuplicateRate)
	str += fmt.Sprintf(", bd: %d|%.1fbps|%d", p.BytesDuplicate, p.BitrateDuplicate, p.HeaderBytesDuplicate)

	str += fmt.Sprintf(", pp: %d|%.2f/s", p.PacketsPadding, p.PacketPaddingRate)
	str += fmt.Sprintf(", bp: %d|%.1fbps|%d", p.BytesPadding, p.BitratePadding, p.HeaderBytesPadding)

	str += fmt.Sprintf(", o: %d", p.PacketsOutOfOrder)

	jitter := r.jitter
	maxJitter := r.maxJitter
	if r.params.IsReceiverReportDriven {
		// NOTE: jitter includes jitter from publisher and from processing
		jitter = r.jitterOverridden
		maxJitter = r.maxJitterOverridden
	}
	str += fmt.Sprintf(", c: %d, j: %d(%.1fus)|%d(%.1fus)", r.params.ClockRate, uint32(jitter), p.JitterCurrent, uint32(maxJitter), p.JitterMax)

	if len(p.GapHistogram) != 0 {
		first := true
		str += ", gh:["
		for burst, count := range p.GapHistogram {
			if !first {
				str += ", "
			}
			first = false
			str += fmt.Sprintf("%d:%d", burst, count)
		}
		str += "]"
	}

	str += ", n:"
	str += fmt.Sprintf("%d|%d|%d|%d", p.Nacks, p.NackAcks, p.NackMisses, p.NackRepeated)

	str += ", pli:"
	str += fmt.Sprintf("%d|%+v / %d|%+v",
		p.Plis, p.LastPli.AsTime().Format(time.UnixDate),
		p.LayerLockPlis, p.LastLayerLockPli.AsTime().Format(time.UnixDate),
	)

	str += ", fir:"
	str += fmt.Sprintf("%d|%+v", p.Firs, p.LastFir.AsTime().Format(time.UnixDate))

	str += ", rtt(ms):"
	str += fmt.Sprintf("%d|%d", p.RttCurrent, p.RttMax)

	return str
}

func (r *RTPStats) ToProto() *livekit.RTPStats {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.startTime.IsZero() {
		return nil
	}

	endTime := r.endTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	elapsed := endTime.Sub(r.startTime).Seconds()
	if elapsed == 0.0 {
		return nil
	}

	packets := r.getTotalPacketsPrimary()
	packetRate := float64(packets) / elapsed
	bitrate := float64(r.bytes) * 8.0 / elapsed

	frameRate := float64(r.frames) / elapsed

	packetsExpected := r.getExtHighestSN() - r.extStartSN + 1
	packetsLost := r.getPacketsLost()
	packetLostRate := float64(packetsLost) / elapsed
	packetLostPercentage := float32(packetsLost) / float32(packetsExpected) * 100.0

	packetDuplicateRate := float64(r.packetsDuplicate) / elapsed
	bitrateDuplicate := float64(r.bytesDuplicate) * 8.0 / elapsed

	packetPaddingRate := float64(r.packetsPadding) / elapsed
	bitratePadding := float64(r.bytesPadding) * 8.0 / elapsed

	jitter := r.jitter
	maxJitter := r.maxJitter
	if r.params.IsReceiverReportDriven {
		// NOTE: jitter includes jitter from publisher and from processing
		jitter = r.jitterOverridden
		maxJitter = r.maxJitterOverridden
	}
	jitterTime := jitter / float64(r.params.ClockRate) * 1e6
	maxJitterTime := maxJitter / float64(r.params.ClockRate) * 1e6

	p := &livekit.RTPStats{
		StartTime:            timestamppb.New(r.startTime),
		EndTime:              timestamppb.New(endTime),
		Duration:             elapsed,
		Packets:              packets,
		PacketRate:           packetRate,
		Bytes:                r.bytes,
		HeaderBytes:          r.headerBytes,
		Bitrate:              bitrate,
		PacketsLost:          packetsLost,
		PacketLossRate:       packetLostRate,
		PacketLossPercentage: packetLostPercentage,
		PacketsDuplicate:     r.packetsDuplicate,
		PacketDuplicateRate:  packetDuplicateRate,
		BytesDuplicate:       r.bytesDuplicate,
		HeaderBytesDuplicate: r.headerBytesDuplicate,
		BitrateDuplicate:     bitrateDuplicate,
		PacketsPadding:       r.packetsPadding,
		PacketPaddingRate:    packetPaddingRate,
		BytesPadding:         r.bytesPadding,
		HeaderBytesPadding:   r.headerBytesPadding,
		BitratePadding:       bitratePadding,
		PacketsOutOfOrder:    r.packetsOutOfOrder,
		Frames:               r.frames,
		FrameRate:            frameRate,
		KeyFrames:            r.keyFrames,
		LastKeyFrame:         timestamppb.New(r.lastKeyFrame),
		JitterCurrent:        jitterTime,
		JitterMax:            maxJitterTime,
		Nacks:                r.nacks,
		NackAcks:             r.nackAcks,
		NackMisses:           r.nackMisses,
		NackRepeated:         r.nackRepeated,
		Plis:                 r.plis,
		LastPli:              timestamppb.New(r.lastPli),
		LayerLockPlis:        r.layerLockPlis,
		LastLayerLockPli:     timestamppb.New(r.lastLayerLockPli),
		Firs:                 r.firs,
		LastFir:              timestamppb.New(r.lastFir),
		RttCurrent:           r.rtt,
		RttMax:               r.maxRtt,
	}

	gapsPresent := false
	for i := 0; i < len(r.gapHistogram); i++ {
		if r.gapHistogram[i] == 0 {
			continue
		}

		gapsPresent = true
		break
	}

	if gapsPresent {
		p.GapHistogram = make(map[int32]uint32, GapHistogramNumBins)
		for i := 0; i < len(r.gapHistogram); i++ {
			if r.gapHistogram[i] == 0 {
				continue
			}

			p.GapHistogram[int32(i+1)] = r.gapHistogram[i]
		}
	}

	return p
}

func (r *RTPStats) getExtHighestSN() uint32 {
	return (uint32(r.cycles) << 16) | uint32(r.highestSN)
}

func (r *RTPStats) getExtHighestSNAdjusted() uint32 {
	if r.params.IsReceiverReportDriven && !r.lastRRTime.IsZero() {
		return r.extHighestSNOverridden
	}

	return r.getExtHighestSN()
}

func (r *RTPStats) getPacketsLost() uint32 {
	if r.params.IsReceiverReportDriven && !r.lastRRTime.IsZero() {
		return r.packetsLostOverridden
	}

	return r.packetsLost
}

func (r *RTPStats) getSnInfoOutOfOrderPtr(sn uint16) int {
	offset := sn - r.highestSN
	if offset > 0 && offset < (1<<15) {
		return -1 // in-order, not expected, maybe too new
	}

	offset = r.highestSN - sn
	if int(offset) >= SnInfoSize {
		// too old, ignore
		return -1
	}

	return (r.snInfoWritePtr - int(offset) - 1) & SnInfoMask
}

func (r *RTPStats) setSnInfo(sn uint16, pktSize uint16, hdrSize uint16, payloadSize uint16, marker bool) {
	writePtr := 0
	ooo := (sn - r.highestSN) > (1 << 15)
	if !ooo {
		writePtr = r.snInfoWritePtr
		r.snInfoWritePtr = (writePtr + 1) & SnInfoMask
	} else {
		writePtr = r.getSnInfoOutOfOrderPtr(sn)
		if writePtr < 0 {
			return
		}
	}

	snInfo := &r.snInfos[writePtr]
	snInfo.pktSize = pktSize
	snInfo.hdrSize = hdrSize
	snInfo.isPaddingOnly = payloadSize == 0
	snInfo.marker = marker
}

func (r *RTPStats) clearSnInfos(startInclusive uint16, endExclusive uint16) {
	for sn := startInclusive; sn != endExclusive; sn++ {
		snInfo := &r.snInfos[r.snInfoWritePtr]
		snInfo.pktSize = 0
		snInfo.hdrSize = 0
		snInfo.isPaddingOnly = false
		snInfo.marker = false

		r.snInfoWritePtr = (r.snInfoWritePtr + 1) & SnInfoMask
	}
}

func (r *RTPStats) isSnInfoLost(sn uint16) bool {
	readPtr := r.getSnInfoOutOfOrderPtr(sn)
	if readPtr < 0 {
		return false
	}

	snInfo := &r.snInfos[readPtr]
	return snInfo.pktSize == 0
}

func (r *RTPStats) getIntervalStats(startInclusive uint16, endExclusive uint16) (intervalStats IntervalStats) {
	packetsNotFound := uint32(0)
	processSN := func(sn uint16) {
		readPtr := r.getSnInfoOutOfOrderPtr(sn)
		if readPtr < 0 {
			packetsNotFound++
			return
		}

		snInfo := &r.snInfos[readPtr]
		switch {
		case snInfo.pktSize == 0:
			intervalStats.packetsLost++

		case snInfo.isPaddingOnly:
			intervalStats.packetsPadding++
			intervalStats.bytesPadding += uint64(snInfo.pktSize)
			intervalStats.headerBytesPadding += uint64(snInfo.hdrSize)

		default:
			intervalStats.packets++
			intervalStats.bytes += uint64(snInfo.pktSize)
			intervalStats.headerBytes += uint64(snInfo.hdrSize)
		}

		if snInfo.marker {
			intervalStats.frames++
		}
	}

	if startInclusive == endExclusive {
		// do a full cycle
		for sn := uint32(0); sn < NumSequenceNumbers; sn++ {
			processSN(uint16(sn))
		}
	} else {
		for sn := startInclusive; sn != endExclusive; sn++ {
			processSN(sn)
		}
	}

	if packetsNotFound != 0 {
		r.logger.Warnw(
			"could not find some packets", nil,
			"start", startInclusive,
			"end", endExclusive,
			"count", packetsNotFound,
			"highestSN", r.highestSN,
		)
	}
	return
}

func (r *RTPStats) updateJitter(rtph *rtp.Header, packetTime time.Time) {
	// Do not update jitter on multiple packets of same frame.
	// All packets of a frame have the same time stamp.
	// NOTE: This does not protect against using more than one packet of the same frame
	//       if packets arrive out-of-order. For example,
	//          p1f1 -> p1f2 -> p2f1
	//       In this case, p2f1 (packet 2, frame 1) will still be used in jitter calculation
	//       although it is the second packet of a frame because of out-of-order receival.
	if r.lastJitterRTP == rtph.Timestamp {
		return
	}

	timeSinceFirst := packetTime.Sub(r.firstTime)
	packetTimeRTP := uint32(timeSinceFirst.Nanoseconds() * int64(r.params.ClockRate) / 1e9)
	transit := packetTimeRTP - rtph.Timestamp

	if r.lastTransit != 0 {
		d := int32(transit - r.lastTransit)
		if d < 0 {
			d = -d
		}
		r.jitter += (float64(d) - r.jitter) / 16
		if r.jitter > r.maxJitter {
			r.maxJitter = r.jitter
		}

		for _, s := range r.snapshots {
			if r.jitter > s.maxJitter {
				s.maxJitter = r.jitter
			}
		}
	}

	r.lastTransit = transit
	r.lastJitterRTP = rtph.Timestamp
}

func (r *RTPStats) updateGapHistogram(gap int) {
	if gap < 2 {
		return
	}

	missing := gap - 1
	if missing > len(r.gapHistogram) {
		r.gapHistogram[len(r.gapHistogram)-1]++
	} else {
		r.gapHistogram[missing-1]++
	}
}

func (r *RTPStats) getAndResetSnapshot(snapshotId uint32, override bool) (*Snapshot, *Snapshot) {
	if !r.initialized || (r.params.IsReceiverReportDriven && r.lastRRTime.IsZero()) {
		return nil, nil
	}

	then := r.snapshots[snapshotId]
	if then == nil {
		then = &Snapshot{
			startTime:            r.startTime,
			extStartSN:           r.extStartSN,
			extStartSNOverridden: r.extStartSN,
		}
		r.snapshots[snapshotId] = then
	}

	var startTime time.Time
	if override && r.params.IsReceiverReportDriven {
		startTime = r.lastRRTime
	} else {
		startTime = time.Now()
	}

	// snapshot now
	r.snapshots[snapshotId] = &Snapshot{
		startTime:             startTime,
		extStartSN:            r.getExtHighestSN() + 1,
		extStartSNOverridden:  r.getExtHighestSNAdjusted() + 1,
		packetsDuplicate:      r.packetsDuplicate,
		bytesDuplicate:        r.bytesDuplicate,
		headerBytesDuplicate:  r.headerBytesDuplicate,
		packetsLostOverridden: r.packetsLostOverridden,
		nacks:                 r.nacks,
		plis:                  r.plis,
		firs:                  r.firs,
		maxJitter:             r.jitter,
		maxJitterOverridden:   r.jitterOverridden,
		maxRtt:                r.rtt,
	}
	// make a copy so that it can be used independently
	now := *r.snapshots[snapshotId]

	return then, &now
}

// ----------------------------------

func getExtTS(ts uint32, cycles uint32) uint64 {
	return (uint64(cycles) << 32) | uint64(ts)
}

func AggregateRTPStats(statsList []*livekit.RTPStats) *livekit.RTPStats {
	if len(statsList) == 0 {
		return nil
	}

	startTime := time.Time{}
	endTime := time.Time{}

	packets := uint32(0)
	bytes := uint64(0)
	headerBytes := uint64(0)
	packetsLost := uint32(0)
	packetsDuplicate := uint32(0)
	bytesDuplicate := uint64(0)
	headerBytesDuplicate := uint64(0)
	packetsPadding := uint32(0)
	bytesPadding := uint64(0)
	headerBytesPadding := uint64(0)
	packetsOutOfOrder := uint32(0)
	frames := uint32(0)
	keyFrames := uint32(0)
	lastKeyFrame := time.Time{}
	jitter := 0.0
	maxJitter := float64(0)
	gapHistogram := make(map[int32]uint32, GapHistogramNumBins)
	nacks := uint32(0)
	nackAcks := uint32(0)
	nackMisses := uint32(0)
	nackRepeated := uint32(0)
	plis := uint32(0)
	lastPli := time.Time{}
	layerLockPlis := uint32(0)
	lastLayerLockPli := time.Time{}
	firs := uint32(0)
	lastFir := time.Time{}
	rtt := uint32(0)
	maxRtt := uint32(0)

	for _, stats := range statsList {
		if startTime.IsZero() || startTime.After(stats.StartTime.AsTime()) {
			startTime = stats.StartTime.AsTime()
		}

		if endTime.IsZero() || endTime.Before(stats.EndTime.AsTime()) {
			endTime = stats.EndTime.AsTime()
		}

		packets += stats.Packets
		bytes += stats.Bytes
		headerBytes += stats.HeaderBytes

		packetsLost += stats.PacketsLost

		packetsDuplicate += stats.PacketsDuplicate
		bytesDuplicate += stats.BytesDuplicate
		headerBytesDuplicate += stats.HeaderBytesDuplicate

		packetsPadding += stats.PacketsPadding
		bytesPadding += stats.BytesPadding
		headerBytesPadding += stats.HeaderBytesPadding

		packetsOutOfOrder += stats.PacketsOutOfOrder

		frames += stats.Frames

		keyFrames += stats.KeyFrames
		if lastKeyFrame.IsZero() || lastKeyFrame.Before(stats.LastKeyFrame.AsTime()) {
			lastKeyFrame = stats.LastKeyFrame.AsTime()
		}

		jitter += stats.JitterCurrent
		if stats.JitterMax > maxJitter {
			maxJitter = stats.JitterMax
		}

		for burst, count := range stats.GapHistogram {
			gapHistogram[burst] += count
		}

		nacks += stats.Nacks
		nackAcks += stats.NackAcks
		nackMisses += stats.NackMisses
		nackRepeated += stats.NackRepeated

		plis += stats.Plis
		if lastPli.IsZero() || lastPli.Before(stats.LastPli.AsTime()) {
			lastPli = stats.LastPli.AsTime()
		}

		layerLockPlis += stats.LayerLockPlis
		if lastLayerLockPli.IsZero() || lastLayerLockPli.Before(stats.LastLayerLockPli.AsTime()) {
			lastLayerLockPli = stats.LastLayerLockPli.AsTime()
		}

		firs += stats.Firs
		if lastFir.IsZero() || lastPli.Before(stats.LastFir.AsTime()) {
			lastFir = stats.LastFir.AsTime()
		}

		rtt += stats.RttCurrent
		if stats.RttMax > maxRtt {
			maxRtt = stats.RttMax
		}
	}

	if endTime.IsZero() {
		endTime = time.Now()
	}
	elapsed := endTime.Sub(startTime).Seconds()

	packetLostRate := float64(packetsLost) / elapsed
	packetLostPercentage := float32(packetsLost) / (float32(packets) + float32(packetsLost)) * 100.0

	packetRate := float64(packets) / elapsed
	packetDuplicateRate := float64(packetsDuplicate) / elapsed
	packetPaddingRate := float64(packetsPadding) / elapsed

	bitrate := float64(bytes) * 8.0 / elapsed
	bitrateDuplicate := float64(bytesDuplicate) * 8.0 / elapsed
	bitratePadding := float64(bytesPadding) * 8.0 / elapsed

	frameRate := float64(frames) / elapsed

	return &livekit.RTPStats{
		StartTime:            timestamppb.New(startTime),
		EndTime:              timestamppb.New(endTime),
		Duration:             elapsed,
		Packets:              packets,
		PacketRate:           packetRate,
		Bytes:                bytes,
		HeaderBytes:          headerBytes,
		Bitrate:              bitrate,
		PacketsLost:          packetsLost,
		PacketLossRate:       packetLostRate,
		PacketLossPercentage: packetLostPercentage,
		PacketsDuplicate:     packetsDuplicate,
		PacketDuplicateRate:  packetDuplicateRate,
		BytesDuplicate:       bytesDuplicate,
		HeaderBytesDuplicate: headerBytesDuplicate,
		BitrateDuplicate:     bitrateDuplicate,
		PacketsPadding:       packetsPadding,
		PacketPaddingRate:    packetPaddingRate,
		BytesPadding:         bytesPadding,
		HeaderBytesPadding:   headerBytesPadding,
		BitratePadding:       bitratePadding,
		PacketsOutOfOrder:    packetsOutOfOrder,
		Frames:               frames,
		FrameRate:            frameRate,
		KeyFrames:            keyFrames,
		LastKeyFrame:         timestamppb.New(lastKeyFrame),
		JitterCurrent:        jitter / float64(len(statsList)),
		JitterMax:            maxJitter,
		GapHistogram:         gapHistogram,
		Nacks:                nacks,
		NackAcks:             nackAcks,
		NackMisses:           nackMisses,
		NackRepeated:         nackRepeated,
		Plis:                 plis,
		LastPli:              timestamppb.New(lastPli),
		LayerLockPlis:        layerLockPlis,
		LastLayerLockPli:     timestamppb.New(lastLayerLockPli),
		Firs:                 firs,
		LastFir:              timestamppb.New(lastFir),
		RttCurrent:           rtt / uint32(len(statsList)),
		RttMax:               maxRtt,
	}
}

func AggregateRTPDeltaInfo(deltaInfoList []*RTPDeltaInfo) *RTPDeltaInfo {
	if len(deltaInfoList) == 0 {
		return nil
	}

	startTime := time.Time{}
	endTime := time.Time{}

	packets := uint32(0)
	bytes := uint64(0)
	headerBytes := uint64(0)

	packetsDuplicate := uint32(0)
	bytesDuplicate := uint64(0)
	headerBytesDuplicate := uint64(0)

	packetsPadding := uint32(0)
	bytesPadding := uint64(0)
	headerBytesPadding := uint64(0)

	packetsLost := uint32(0)
	packetsMissing := uint32(0)

	frames := uint32(0)

	maxRtt := uint32(0)
	maxJitter := float64(0)

	nacks := uint32(0)
	plis := uint32(0)
	firs := uint32(0)

	for _, deltaInfo := range deltaInfoList {
		if deltaInfo == nil {
			continue
		}

		if startTime.IsZero() || startTime.After(deltaInfo.StartTime) {
			startTime = deltaInfo.StartTime
		}

		endedAt := deltaInfo.StartTime.Add(deltaInfo.Duration)
		if endTime.IsZero() || endTime.Before(endedAt) {
			endTime = endedAt
		}

		packets += deltaInfo.Packets
		bytes += deltaInfo.Bytes
		headerBytes += deltaInfo.HeaderBytes

		packetsDuplicate += deltaInfo.PacketsDuplicate
		bytesDuplicate += deltaInfo.BytesDuplicate
		headerBytesDuplicate += deltaInfo.HeaderBytesDuplicate

		packetsPadding += deltaInfo.PacketsPadding
		bytesPadding += deltaInfo.BytesPadding
		headerBytesPadding += deltaInfo.HeaderBytesPadding

		packetsLost += deltaInfo.PacketsLost
		packetsMissing += deltaInfo.PacketsMissing

		frames += deltaInfo.Frames

		if deltaInfo.RttMax > maxRtt {
			maxRtt = deltaInfo.RttMax
		}

		if deltaInfo.JitterMax > maxJitter {
			maxJitter = deltaInfo.JitterMax
		}

		nacks += deltaInfo.Nacks
		plis += deltaInfo.Plis
		firs += deltaInfo.Firs
	}
	if startTime.IsZero() || endTime.IsZero() {
		return nil
	}

	return &RTPDeltaInfo{
		StartTime:            startTime,
		Duration:             endTime.Sub(startTime),
		Packets:              packets,
		Bytes:                bytes,
		HeaderBytes:          headerBytes,
		PacketsDuplicate:     packetsDuplicate,
		BytesDuplicate:       bytesDuplicate,
		HeaderBytesDuplicate: headerBytesDuplicate,
		PacketsPadding:       packetsPadding,
		BytesPadding:         bytesPadding,
		HeaderBytesPadding:   headerBytesPadding,
		PacketsLost:          packetsLost,
		PacketsMissing:       packetsMissing,
		Frames:               frames,
		RttMax:               maxRtt,
		JitterMax:            maxJitter,
		Nacks:                nacks,
		Plis:                 plis,
		Firs:                 firs,
	}
}

// -------------------------------------------------------------------

type PIDController struct {
	kp, ki, kd float64

	tau float64 // low pass filter of D, time constant

	outMin, outMax float64
	isOutLimitsSet bool

	iMin, iMax   float64
	isILimitsSet bool

	iVal, dVal float64

	prevError, prevMeasurement float64
	prevMeasurementTime        time.Time
}

func NewPIDController() *PIDController {
	return &PIDController{}
}

func (p *PIDController) SetGains(kp, ki, kd float64) {
	p.kp = kp
	p.ki = ki
	p.kd = kd
}

func (p *PIDController) SetDerivativeLPF(tau float64) {
	p.tau = tau
}

func (p *PIDController) SetOutputLimits(min, max float64) {
	p.outMin = min
	p.outMax = max
	p.isOutLimitsSet = true
}

func (p *PIDController) SetIntegralLimits(min, max float64) {
	p.iMin = min
	p.iMax = max
	p.isILimitsSet = true
}

func (p *PIDController) Update(setpoint, measurement float64, at time.Time) float64 {
	diff := setpoint - measurement
	if p.prevMeasurementTime.IsZero() {
		p.prevError = diff
		p.prevMeasurement = measurement
		p.prevMeasurementTime = at
		return 0
	}

	proportional := p.kp * diff

	duration := at.Sub(p.prevMeasurementTime).Seconds()
	p.iVal = p.iVal + (0.5 * p.ki * duration * (diff + p.prevError))
	if p.isILimitsSet {
		if p.iVal > p.iMax {
			p.iVal = p.iMax
		}
		if p.iVal < p.iMin {
			p.iVal = p.iMin
		}
	}

	p.dVal = (-2.0 * p.kd * (measurement - p.prevMeasurement)) + (((2.0*p.tau - duration) * p.dVal) / (2.0*p.tau + duration))

	output := proportional + p.iVal + p.dVal
	if p.isOutLimitsSet {
		if output > p.outMax {
			output = p.outMax
		}
		if output < p.outMin {
			output = p.outMin
		}
	}

	p.prevError = diff
	p.prevMeasurement = measurement
	p.prevMeasurementTime = at
	return output
}

// -------------------------------------------------------------------
