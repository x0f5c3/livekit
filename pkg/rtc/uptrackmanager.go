package rtc

import (
	"errors"
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/utils"
)

var (
	ErrSubscriptionPermissionNeedsIdentity = errors.New("participant identity not present")
)

type UpTrackManagerParams struct {
	SID    livekit.ParticipantID
	Logger logger.Logger
}

type UpTrackManager struct {
	params UpTrackManagerParams

	closed bool

	// publishedTracks that participant is publishing
	publishedTracks               map[livekit.TrackID]types.MediaTrack
	subscriptionPermission        *livekit.SubscriptionPermission
	subscriptionPermissionVersion *utils.TimedVersion
	// subscriber permission for published tracks
	subscriberPermissions map[livekit.ParticipantIdentity]*livekit.TrackPermission
	// keeps tracks of track specific subscribers who are awaiting permission
	pendingSubscriptions map[livekit.TrackID][]types.LocalParticipant

	opsQueue *utils.OpsQueue

	lock sync.RWMutex

	// callbacks & handlers
	onClose        func()
	onTrackUpdated func(track types.MediaTrack, onlyIfReady bool)
}

func NewUpTrackManager(params UpTrackManagerParams) *UpTrackManager {
	return &UpTrackManager{
		params:               params,
		publishedTracks:      make(map[livekit.TrackID]types.MediaTrack),
		pendingSubscriptions: make(map[livekit.TrackID][]types.LocalParticipant),
		opsQueue:             utils.NewOpsQueue(params.Logger, "utm", 20),
	}
}

func (u *UpTrackManager) Start() {
	u.opsQueue.Start()
}

func (u *UpTrackManager) Close(willBeResumed bool) {
	u.opsQueue.Stop()

	u.lock.Lock()
	u.closed = true
	notify := len(u.publishedTracks) == 0
	u.lock.Unlock()

	// remove all subscribers
	for _, t := range u.GetPublishedTracks() {
		t.ClearAllReceivers(willBeResumed)
	}

	if notify && u.onClose != nil {
		u.onClose()
	}
}

func (u *UpTrackManager) OnUpTrackManagerClose(f func()) {
	u.onClose = f
}

func (u *UpTrackManager) ToProto() []*livekit.TrackInfo {
	u.lock.RLock()
	defer u.lock.RUnlock()

	var trackInfos []*livekit.TrackInfo
	for _, t := range u.publishedTracks {
		trackInfos = append(trackInfos, t.ToProto())
	}

	return trackInfos
}

func (u *UpTrackManager) OnPublishedTrackUpdated(f func(track types.MediaTrack, onlyIfReady bool)) {
	u.onTrackUpdated = f
}

// AddSubscriber subscribes op to all publishedTracks
func (u *UpTrackManager) AddSubscriber(sub types.LocalParticipant, params types.AddSubscriberParams) (int, error) {
	u.lock.Lock()
	defer u.lock.Unlock()

	var tracks []types.MediaTrack
	if params.AllTracks {
		for _, t := range u.publishedTracks {
			tracks = append(tracks, t)
		}
	} else {
		for _, trackID := range params.TrackIDs {
			track := u.getPublishedTrackLocked(trackID)
			if track == nil {
				continue
			}

			tracks = append(tracks, track)
		}
	}
	if len(tracks) == 0 {
		return 0, nil
	}

	var trackIDs []livekit.TrackID
	for _, track := range tracks {
		trackIDs = append(trackIDs, track.ID())
	}
	u.params.Logger.Errorw("subscribing participant to tracks", nil,
		"subscriber", sub.Identity(),
		"subscriberID", sub.ID(),
		"trackIDs", trackIDs)

	n := 0
	for _, track := range tracks {
		trackID := track.ID()
		subscriberIdentity := sub.Identity()
		if !u.hasPermissionLocked(trackID, subscriberIdentity) {
			u.maybeAddPendingSubscriptionLocked(trackID, sub)
			continue
		}

		if err := track.AddSubscriber(sub); err != nil {
			return n, err
		}
		n += 1

		u.maybeRemovePendingSubscriptionLocked(trackID, sub, true)
	}
	return n, nil
}

func (u *UpTrackManager) RemoveSubscriber(sub types.LocalParticipant, trackID livekit.TrackID, willBeResumed bool) {
	u.lock.Lock()
	defer u.lock.Unlock()

	track := u.getPublishedTrackLocked(trackID)
	if track != nil {
		track.RemoveSubscriber(sub.ID(), willBeResumed)
	}

	u.maybeRemovePendingSubscriptionLocked(trackID, sub, false)
}

func (u *UpTrackManager) SetPublishedTrackMuted(trackID livekit.TrackID, muted bool) types.MediaTrack {
	u.lock.RLock()
	track := u.publishedTracks[trackID]
	u.lock.RUnlock()

	if track != nil {
		currentMuted := track.IsMuted()
		track.SetMuted(muted)

		if currentMuted != track.IsMuted() {
			u.params.Logger.Infow("publisher mute status changed", "trackID", trackID, "muted", track.IsMuted())
			if u.onTrackUpdated != nil {
				u.onTrackUpdated(track, false)
			}
		}
	}

	return track
}

func (u *UpTrackManager) GetPublishedTrack(trackID livekit.TrackID) types.MediaTrack {
	u.lock.RLock()
	defer u.lock.RUnlock()

	return u.getPublishedTrackLocked(trackID)
}

func (u *UpTrackManager) GetPublishedTracks() []types.MediaTrack {
	u.lock.RLock()
	defer u.lock.RUnlock()

	tracks := make([]types.MediaTrack, 0, len(u.publishedTracks))
	for _, t := range u.publishedTracks {
		tracks = append(tracks, t)
	}
	return tracks
}

func (u *UpTrackManager) UpdateSubscriptionPermission(
	subscriptionPermission *livekit.SubscriptionPermission,
	timedVersion *livekit.TimedVersion,
) error {
	u.lock.Lock()
	defer u.lock.Unlock()

	if timedVersion != nil {
		if u.subscriptionPermissionVersion != nil {
			tv := utils.NewTimedVersionFromProto(timedVersion)
			// ignore older version
			if !tv.After(u.subscriptionPermissionVersion) {
				perms := ""
				if u.subscriptionPermission != nil {
					perms = u.subscriptionPermission.String()
				}
				u.params.Logger.Infow(
					"skipping older subscription permission version",
					"existingValue", perms,
					"existingVersion", u.subscriptionPermissionVersion.ToProto().String(),
					"requestingValue", subscriptionPermission.String(),
					"requestingVersion", timedVersion.String(),
				)
				return nil
			}
			u.subscriptionPermissionVersion.Update(time.UnixMicro(timedVersion.UnixMicro))
		} else {
			u.subscriptionPermissionVersion = utils.NewTimedVersionFromProto(timedVersion)
		}
	} else {
		// use current time as the new/updated version
		if u.subscriptionPermissionVersion == nil {
			u.subscriptionPermissionVersion = utils.NewTimedVersion(time.Now(), 0)
		} else {
			u.subscriptionPermissionVersion.Update(time.Now())
		}
	}

	// store as is for use when migrating
	u.subscriptionPermission = subscriptionPermission
	if subscriptionPermission == nil {
		u.params.Logger.Debugw(
			"updating subscription permission, setting to nil",
			"version", u.subscriptionPermissionVersion.ToProto().String(),
		)
		// possible to get a nil when migrating
		return nil
	}

	u.params.Logger.Debugw(
		"updating subscription permission",
		"permissions", u.subscriptionPermission.String(),
		"version", u.subscriptionPermissionVersion.ToProto().String(),
	)
	if err := u.parseSubscriptionPermissionsLocked(subscriptionPermission); err != nil {
		// when failed, do not override previous permissions
		u.params.Logger.Errorw("failed updating subscription permission", err)
		return err
	}

	u.processPendingSubscriptionsLocked()
	u.maybeRevokeSubscriptionsLocked()

	return nil
}

func (u *UpTrackManager) SubscriptionPermission() (*livekit.SubscriptionPermission, *livekit.TimedVersion) {
	u.lock.RLock()
	defer u.lock.RUnlock()

	if u.subscriptionPermissionVersion == nil {
		return nil, nil
	}

	return u.subscriptionPermission, u.subscriptionPermissionVersion.ToProto()
}

func (u *UpTrackManager) UpdateVideoLayers(updateVideoLayers *livekit.UpdateVideoLayers) error {
	track := u.GetPublishedTrack(livekit.TrackID(updateVideoLayers.TrackSid))
	if track == nil {
		u.params.Logger.Warnw("could not find track", nil, "trackID", livekit.TrackID(updateVideoLayers.TrackSid))
		return errors.New("could not find published track")
	}

	track.UpdateVideoLayers(updateVideoLayers.Layers)
	if u.onTrackUpdated != nil {
		u.onTrackUpdated(track, false)
	}

	return nil
}

func (u *UpTrackManager) AddPublishedTrack(track types.MediaTrack) {
	u.lock.Lock()
	if _, ok := u.publishedTracks[track.ID()]; !ok {
		u.publishedTracks[track.ID()] = track
	}
	u.lock.Unlock()
	u.params.Logger.Debugw("added published track", "trackID", track.ID(), "trackInfo", track.ToProto().String())

	track.AddOnClose(func() {
		notifyClose := false

		// cleanup
		u.lock.Lock()
		trackID := track.ID()
		delete(u.publishedTracks, trackID)
		delete(u.pendingSubscriptions, trackID)
		// not modifying subscription permissions, will get reset on next update from participant

		if u.closed && len(u.publishedTracks) == 0 {
			notifyClose = true
		}
		u.lock.Unlock()

		// only send this when client is in a ready state
		if u.onTrackUpdated != nil {
			u.onTrackUpdated(track, true)
		}

		if notifyClose && u.onClose != nil {
			u.onClose()
		}
	})
}

func (u *UpTrackManager) RemovePublishedTrack(track types.MediaTrack, willBeResumed bool, shouldClose bool) {
	if shouldClose {
		track.Close(willBeResumed)
	} else {
		track.ClearAllReceivers(willBeResumed)
	}
	u.lock.Lock()
	delete(u.publishedTracks, track.ID())
	u.lock.Unlock()
}

func (u *UpTrackManager) getPublishedTrackLocked(trackID livekit.TrackID) types.MediaTrack {
	return u.publishedTracks[trackID]
}

func (u *UpTrackManager) parseSubscriptionPermissionsLocked(subscriptionPermission *livekit.SubscriptionPermission) error {
	// every update overrides the existing

	// all_participants takes precedence
	if subscriptionPermission.AllParticipants {
		// everything is allowed, nothing else to do
		u.subscriberPermissions = nil
		return nil
	}

	// per participant permissions
	subscriberPermissions := make(map[livekit.ParticipantIdentity]*livekit.TrackPermission)
	for _, trackPerms := range subscriptionPermission.TrackPermissions {
		subscriberIdentity := livekit.ParticipantIdentity(trackPerms.ParticipantIdentity)
		if subscriberIdentity == "" {
			return ErrSubscriptionPermissionNeedsIdentity
		}

		subscriberPermissions[subscriberIdentity] = trackPerms
	}

	u.subscriberPermissions = subscriberPermissions

	return nil
}

func (u *UpTrackManager) hasPermissionLocked(trackID livekit.TrackID, subscriberIdentity livekit.ParticipantIdentity) bool {
	if u.subscriberPermissions == nil {
		return true
	}

	perms, ok := u.subscriberPermissions[subscriberIdentity]
	if !ok {
		return false
	}

	if perms.AllTracks {
		return true
	}

	for _, sid := range perms.TrackSids {
		if livekit.TrackID(sid) == trackID {
			return true
		}
	}

	return false
}

func (u *UpTrackManager) getAllowedSubscribersLocked(trackID livekit.TrackID) []livekit.ParticipantIdentity {
	if u.subscriberPermissions == nil {
		return nil
	}

	allowed := make([]livekit.ParticipantIdentity, 0)
	for subscriberIdentity, perms := range u.subscriberPermissions {
		if perms.AllTracks {
			allowed = append(allowed, subscriberIdentity)
			continue
		}

		for _, sid := range perms.TrackSids {
			if livekit.TrackID(sid) == trackID {
				allowed = append(allowed, subscriberIdentity)
				break
			}
		}
	}

	return allowed
}

func (u *UpTrackManager) maybeAddPendingSubscriptionLocked(trackID livekit.TrackID, sub types.LocalParticipant) {
	pending := u.pendingSubscriptions[trackID]
	for _, s := range pending {
		if s == sub {
			// already pending
			return
		}
	}

	u.pendingSubscriptions[trackID] = append(u.pendingSubscriptions[trackID], sub)
	u.params.Logger.Debugw("adding pending subscription", "subscriberIdentity", sub.Identity(), "trackID", trackID)
	u.opsQueue.Enqueue(func() {
		sub.SubscriptionPermissionUpdate(u.params.SID, trackID, false)
	})
}

func (u *UpTrackManager) maybeRemovePendingSubscriptionLocked(trackID livekit.TrackID, sub types.LocalParticipant, sendUpdate bool) {
	u.params.Logger.Debugw("maybe remove pending subscriptions", "pending", u.pendingSubscriptions, "sendUpdate", sendUpdate) // REMOVE

	found := false

	pending := u.pendingSubscriptions[trackID]
	n := len(pending)
	for idx, s := range pending {
		if s == sub {
			found = true
			u.pendingSubscriptions[trackID][idx] = u.pendingSubscriptions[trackID][n-1]
			u.pendingSubscriptions[trackID] = u.pendingSubscriptions[trackID][:n-1]
			break
		}
	}
	if len(u.pendingSubscriptions[trackID]) == 0 {
		delete(u.pendingSubscriptions, trackID)
	}

	if found && sendUpdate {
		u.params.Logger.Debugw("removing pending subscription", "subscriberID", sub.ID(), "trackID", trackID)
		u.opsQueue.Enqueue(func() {
			sub.SubscriptionPermissionUpdate(u.params.SID, trackID, true)
		})
	}
}

// creates subscriptions for tracks if permissions have been granted
func (u *UpTrackManager) processPendingSubscriptionsLocked() {
	// check for subscriptions that can be reinstated
	updatedPendingSubscriptions := make(map[livekit.TrackID][]types.LocalParticipant)
	for trackID, pending := range u.pendingSubscriptions {
		track := u.getPublishedTrackLocked(trackID)
		if track == nil {
			u.params.Logger.Debugw("published track gone", "trackID", trackID) // REMOVE
			// published track is gone
			continue
		}

		var updatedPending []types.LocalParticipant
		for _, sub := range pending {
			if sub.State() == livekit.ParticipantInfo_DISCONNECTED {
				// do not keep this pending subscription as subscriber is not connected
				u.params.Logger.Debugw("sub disconnected", "trackID", trackID, "identity", sub.Identity()) // REMOVE
				continue
			}

			if !u.hasPermissionLocked(trackID, sub.Identity()) {
				updatedPending = append(updatedPending, sub)
				u.params.Logger.Debugw("no permissions", "trackID", trackID, "identity", sub.Identity()) // REMOVE
				continue
			}

			if err := track.AddSubscriber(sub); err != nil {
				u.params.Logger.Errorw("error reinstating subscription", err,
					"subscriberIdentity", sub.Identity(),
					"subscirberID", sub.ID(),
					"trackID", trackID,
				)
				// keep it in pending on error in case the error is transient
				updatedPending = append(updatedPending, sub)
				continue
			}

			u.params.Logger.Debugw("reinstating subscription",
				"subscriberIdentity", sub.Identity(),
				"subscirberID", sub.ID(),
				"trackID", trackID,
			)
			u.opsQueue.Enqueue(func() {
				sub.SubscriptionPermissionUpdate(u.params.SID, trackID, true)
			})
		}

		updatedPendingSubscriptions[trackID] = updatedPending
	}

	u.pendingSubscriptions = updatedPendingSubscriptions
}

func (u *UpTrackManager) maybeRevokeSubscriptionsLocked() {
	for trackID, track := range u.publishedTracks {
		allowed := u.getAllowedSubscribersLocked(trackID)
		if allowed == nil {
			// no restrictions
			continue
		}

		revoked := track.RevokeDisallowedSubscribers(allowed)
		for _, sub := range revoked {
			u.maybeAddPendingSubscriptionLocked(trackID, sub)
		}
	}
}

func (u *UpTrackManager) DebugInfo() map[string]interface{} {
	info := map[string]interface{}{}
	publishedTrackInfo := make(map[livekit.TrackID]interface{})

	u.lock.RLock()
	for trackID, track := range u.publishedTracks {
		if mt, ok := track.(*MediaTrack); ok {
			publishedTrackInfo[trackID] = mt.DebugInfo()
		} else {
			publishedTrackInfo[trackID] = map[string]interface{}{
				"ID":       track.ID(),
				"Kind":     track.Kind().String(),
				"PubMuted": track.IsMuted(),
			}
		}
	}
	u.lock.RUnlock()

	info["PublishedTracks"] = publishedTrackInfo

	return info
}
