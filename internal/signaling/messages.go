package signaling

import "encoding/json"

func Offer(sessionID, sdp string) map[string]any {
	return map[string]any{
		"type":      "OFFER",
		"sessionId": sessionID,
		"sdp":       sdp,
	}
}

func Answer(sessionID, sdp string) map[string]any {
	return map[string]any{
		"type":      "ANSWER",
		"sessionId": sessionID,
		"sdp":       sdp,
	}
}

func JoinAccepted(sessionID string) map[string]any {
	return map[string]any{
		"type":      "JOIN_ACCEPTED",
		"sessionId": sessionID,
	}
}

func JoinRejected(sessionID string) map[string]any {
	return map[string]any{
		"type":      "JOIN_REJECTED",
		"sessionId": sessionID,
	}
}

func IceCandidate(sessionID, candidate, sdpMid string, sdpMLineIndex int) map[string]any {
	return map[string]any{
		"type":      "ICE_CANDIDATE",
		"sessionId": sessionID,
		"iceCandidate": map[string]any{
			"candidate":     candidate,
			"sdpMid":        sdpMid,
			"sdpMLineIndex": sdpMLineIndex,
		},
	}
}

type IceCandidatePayload struct {
	Candidate     string `json:"candidate"`
	SdpMid        string `json:"sdpMid"`
	SdpMLineIndex int    `json:"sdpMLineIndex"`
}

func ParseIceCandidate(payload map[string]any) (IceCandidatePayload, bool) {
	raw, ok := payload["iceCandidate"]
	if !ok {
		return IceCandidatePayload{}, false
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return IceCandidatePayload{}, false
	}
	var out IceCandidatePayload
	if err := json.Unmarshal(b, &out); err != nil {
		return IceCandidatePayload{}, false
	}
	if out.SdpMid == "" {
		out.SdpMid = "0"
	}
	return out, true
}

func GetSessionID(payload map[string]any) string {
	if v, ok := payload["sessionId"].(string); ok {
		return v
	}
	return ""
}

func GetSDP(payload map[string]any) string {
	if v, ok := payload["sdp"].(string); ok {
		return v
	}
	return ""
}
