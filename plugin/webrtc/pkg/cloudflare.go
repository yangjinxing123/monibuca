package webrtc

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/pion/webrtc/v4"
	"m7s.live/v5"
	pkg "m7s.live/v5/pkg"
)

// Plugin-specific progress step names for WebRTC
const (
	StepWebRTCInit    pkg.StepName = "webrtc_init"
	StepOfferCreate   pkg.StepName = "offer_create"
	StepSessionCreate pkg.StepName = "session_create"
	StepTrackSetup    pkg.StepName = "track_setup"
	StepNegotiation   pkg.StepName = "negotiation"
)

// Fixed steps for WebRTC pull workflow
var webrtcPullSteps = []pkg.StepDef{
	{Name: pkg.StepPublish, Description: "Publishing stream"},
	{Name: StepWebRTCInit, Description: "Initializing WebRTC connection"},
	{Name: StepOfferCreate, Description: "Creating WebRTC offer"},
	{Name: StepSessionCreate, Description: "Creating session with server"},
	{Name: StepTrackSetup, Description: "Setting up media tracks"},
	{Name: StepNegotiation, Description: "Completing WebRTC negotiation"},
	{Name: pkg.StepStreaming, Description: "Receiving media stream"},
}

type (
	CFClient struct {
		MultipleConnection
		pullCtx   m7s.PullJob
		pushCtx   m7s.PushJob
		direction string
		ApiBase   string
		sessionId string
	}
	SessionCreateResponse struct {
		SessionId                 string `json:"sessionId"`
		webrtc.SessionDescription `json:"sessionDescription"`
	}
	TrackInfo struct {
		Location  string `json:"location"`
		TrackName string `json:"trackName"`
		SessionId string `json:"sessionId"`
	}
	TrackRequest struct {
		Tracks []TrackInfo `json:"tracks"`
	}
	NewTrackResponse struct {
		webrtc.SessionDescription      `json:"sessionDescription"`
		Tracks                         []TrackInfo `json:"tracks"`
		RequiresImmediateRenegotiation bool        `json:"requiresImmediateRenegotiation"`
	}
	RenegotiateResponse struct {
		ErrorCode        int    `json:"errorCode"`
		ErrorDescription string `json:"errorDescription"`
	}
	SDPBody struct {
		*webrtc.SessionDescription `json:"sessionDescription"`
	}
)

func NewCFClient(direction string) *CFClient {
	return &CFClient{
		direction: direction,
	}
}

func (c *CFClient) Start() (err error) {
	if c.direction == DIRECTION_PULL {
		// Initialize progress tracking for pull operations
		c.pullCtx.SetProgressStepsDefs(webrtcPullSteps)

		err = c.pullCtx.Publish()
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}

		c.pullCtx.GoToStepConst(StepWebRTCInit)

		c.Publisher = c.pullCtx.Publisher
		u, _ := url.Parse(c.pullCtx.RemoteURL)
		c.ApiBase, _, _ = strings.Cut(c.pullCtx.RemoteURL, "?")
		c.Receive()
		var transeiver *webrtc.RTPTransceiver
		transeiver, err = c.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		})
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}
		c.Info("webrtc add transceiver", "transceiver", transeiver.Mid())

		c.pullCtx.GoToStepConst(StepOfferCreate)

		var sdpBody SDPBody
		sdpBody.SessionDescription, err = c.GetOffer()
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}

		c.pullCtx.GoToStepConst(StepSessionCreate)

		var result SessionCreateResponse
		err = c.request("new", sdpBody, &result)
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}
		err = c.SetRemoteDescription(result.SessionDescription)
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}
		c.sessionId = result.SessionId

		c.pullCtx.GoToStepConst(StepTrackSetup)

		var result2 NewTrackResponse
		err = c.request("tracks/new", TrackRequest{[]TrackInfo{{
			Location:  "remote",
			TrackName: c.Publisher.StreamPath,
			SessionId: u.Query().Get("sessionId"),
		}}}, &result2)
		if err != nil {
			c.pullCtx.Fail(err.Error())
			return
		}
		c.Info("cloudflare pull success", "result", result2)

		c.pullCtx.GoToStepConst(StepNegotiation)

		if result2.RequiresImmediateRenegotiation {
			err = c.PeerConnection.SetRemoteDescription(result2.SessionDescription)
			if err != nil {
				c.pullCtx.Fail(err.Error())
				return
			}
			var renegotiate SDPBody
			renegotiate.SessionDescription, err = c.GetAnswer()
			if err != nil {
				c.pullCtx.Fail(err.Error())
				return
			}
			var result RenegotiateResponse
			err = c.request("renegotiate", renegotiate, &result)
			if err != nil {
				c.pullCtx.Fail(err.Error())
				return err
			}
			c.Info("cloudflare renegotiate", "result", result)
		}

		c.pullCtx.GoToStepConst(pkg.StepStreaming)
	}
	return
}

func (c *CFClient) request(href string, body any, result any) (err error) {
	var req *http.Request
	var res *http.Response
	var bodyBytes []byte
	method := "POST"
	if href == "renegotiate" {
		method = "PUT"
	}
	bodyBytes, err = json.Marshal(body)
	if c.sessionId != "" {
		href = c.sessionId + "/" + href
	}
	href = c.ApiBase + "/sessions/" + href
	c.Debug("cloudflare request", "url", href, "body", string(bodyBytes))
	req, err = http.NewRequestWithContext(c.Context, method, href, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return
	}
	for k, v := range c.pullCtx.Header {
		for _, v := range v {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Content-Type", "application/json")

	res, err = c.pullCtx.HTTPClient.Do(req)
	if err != nil {
		return
	}
	if res.StatusCode >= 400 {
		err = errors.New("http status code " + res.Status)
		return
	}
	err = json.NewDecoder(res.Body).Decode(&result)
	return
}

func (c *CFClient) GetPullJob() *m7s.PullJob {
	return &c.pullCtx
}

func (c *CFClient) GetPushJob() *m7s.PushJob {
	return &c.pushCtx
}
