package sip003

import (
	"context"
	"net"
	"strconv"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2ray"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/smux"
)

// gost v3 SIP003 plugin: websocket (ws/mws) transport, compatible with
// mihomo's gost-plugin (MetaCubeX/mihomo transport/gost) and gost v3 servers.
//
// plugin_opts: mode=websocket;host=example.com;path=/path;mux=1;tls
func init() {
	RegisterPlugin("gost-plugin", newGostPlugin)
	RegisterPlugin("gost", newGostPlugin)
}

func newGostPlugin(ctx context.Context, pluginOpts Args, router adapter.Router, dialer N.Dialer, serverAddr M.Socksaddr) (Plugin, error) {
	mode := "websocket"
	if modeOpt, loaded := pluginOpts.Get("mode"); loaded {
		mode = modeOpt
	}
	if mode != "websocket" {
		return nil, E.New("gost-plugin: unknown mode: " + mode)
	}

	host := "bing.com"
	path := "/"
	if hostOpt, loaded := pluginOpts.Get("host"); loaded {
		host = hostOpt
	}
	if pathOpt, loaded := pluginOpts.Get("path"); loaded {
		path = pathOpt
	}

	var tlsOptions option.OutboundTLSOptions
	if _, loaded := pluginOpts.Get("tls"); loaded {
		tlsOptions.Enabled = true
		tlsOptions.ServerName = host
		tlsOptions.ALPN = []string{"http/1.1"}
	}

	var tlsClient tls.Config
	var err error
	if tlsOptions.Enabled {
		tlsClient, err = tls.NewClient(ctx, serverAddr.AddrString(), tlsOptions)
		if err != nil {
			return nil, err
		}
	}

	headers := map[string]badoption.Listable[string]{
		"Host": []string{host},
	}
	transportOptions := option.V2RayTransportOptions{
		Type: C.V2RayTransportTypeWebsocket,
		WebsocketOptions: option.V2RayWebsocketOptions{
			Headers: headers,
			Path:    path,
		},
	}
	transport, err := v2ray.NewClientTransport(context.Background(), dialer, serverAddr, transportOptions, tlsClient)
	if err != nil {
		return nil, err
	}

	// mws (multiplexed websocket): wrap each connection in an smux session,
	// same as mihomo gost / gost v3 mws. Default on (mihomo default), mux=0 disables.
	mux := true
	if muxOpt, loaded := pluginOpts.Get("mux"); loaded {
		muxVal, err := strconv.Atoi(muxOpt)
		if err != nil {
			return nil, E.Cause(err, "parse mux value")
		}
		mux = muxVal > 0
	}
	if !mux {
		return transport, nil
	}
	return &gostMuxWrapper{transport}, nil
}

var _ Plugin = (*gostMuxWrapper)(nil)

type gostMuxWrapper struct {
	adapter.V2RayClientTransport
}

func (w *gostMuxWrapper) DialContext(ctx context.Context) (net.Conn, error) {
	conn, err := w.V2RayClientTransport.DialContext(ctx)
	if err != nil {
		return nil, err
	}
	config := smux.DefaultConfig()
	config.KeepAliveDisabled = true
	session, err := smux.Client(conn, config)
	if err != nil {
		conn.Close()
		return nil, err
	}
	stream, err := session.OpenStream()
	if err != nil {
		session.Close()
		return nil, err
	}
	return &gostMuxConn{Conn: stream, session: session}, nil
}

type gostMuxConn struct {
	net.Conn
	session *smux.Session
}

func (c *gostMuxConn) Close() error {
	streamErr := c.Conn.Close()
	sessionErr := c.session.Close()
	if streamErr != nil {
		return streamErr
	}
	return sessionErr
}
