package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/nais/tester/lua/reporter"
	"github.com/nais/tester/lua/spec"
	lua "github.com/yuin/gopher-lua"
)

type REST struct {
	server   http.Handler
	headers  http.Header
	response *httptest.ResponseRecorder
}

var _ spec.Runner = (*REST)(nil)

func NewRestRunner(server http.Handler) *REST {
	return &REST{server: server}
}

func (r *REST) Name() string {
	return "rest"
}

func (r *REST) Functions() []*spec.Function {
	return []*spec.Function{
		{
			Name: "addHeader",
			Args: []spec.Argument{
				{
					Name: "key",
					Type: []spec.ArgumentType{spec.ArgumentTypeString},
					Doc:  "The header key",
				},
				{
					Name: "value",
					Type: []spec.ArgumentType{spec.ArgumentTypeString},
					Doc:  "The header value",
				},
			},
			Doc:  "Add a header to the request",
			Func: r.addHeader,
		},
		{
			Name: "send",
			Args: []spec.Argument{
				{
					Name: "method",
					Type: []spec.ArgumentType{spec.StringEnum{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"}},
					Doc:  "HTTP method",
				},
				{
					Name: "path",
					Type: []spec.ArgumentType{spec.ArgumentTypeString},
					Doc:  "The path to query",
				},
				{
					Name: "body?",
					Type: []spec.ArgumentType{spec.ArgumentTypeString, spec.ArgumentTypeTable},
					Doc:  "The body to send",
				},
			},
			Doc:  "Send http request",
			Func: r.send,
		},
		{
			Name: "check",
			Args: []spec.Argument{
				{
					Name: "status_code",
					Type: []spec.ArgumentType{spec.ArgumentTypeNumber},
					Doc:  "Expected status code",
				},
				{
					Name: "resp",
					Type: []spec.ArgumentType{spec.ArgumentTypeTable},
					Doc:  "Expected response",
				},
			},
			Doc:  "Check the response done by send",
			Func: r.check,
		},
	}
}

func (r *REST) send(L *lua.LState) int {
	if r.response != nil {
		r.response = nil
	}

	ctx := L.Context()
	method := L.CheckString(1)
	path := L.CheckString(2)
	var body io.Reader
	var bodyContent string
	if L.GetTop() > 2 {
		switch L.Get(3).(type) {
		case lua.LString:
			bodyContent = L.CheckString(3)
			body = strings.NewReader(bodyContent)
		case *lua.LTable:
			tbl := L.CheckTable(3)
			b, err := json.Marshal(tbl)
			if err != nil {
				L.RaiseError("unable to marshal table: %v", err)
			}
			bodyContent = string(b)
			body = bytes.NewReader(b)
		}
	}

	// Log the request
	requestInfo := fmt.Sprintf("%s %s", method, path)
	if bodyContent != "" {
		requestInfo += "\n\n" + bodyContent
	}
	Info(ctx, reporter.Info{
		Type:     reporter.InfoTypeRequest,
		Title:    "HTTP Request",
		Content:  requestInfo,
		Language: "text",
	})

	req, err := http.NewRequestWithContext(ctx, method, path, body)
	if err != nil {
		panic(fmt.Errorf("rest.Run: unable to create request: %w", err))
	}

	for k := range r.headers {
		req.Header.Add(k, r.headers.Get(k))
	}

	r.response = httptest.NewRecorder()
	r.server.ServeHTTP(r.response, req)

	// Log the response
	Info(ctx, reporter.Info{
		Type:     reporter.InfoTypeResponse,
		Title:    fmt.Sprintf("HTTP Response (%d)", r.response.Code),
		Content:  r.response.Body.String(),
		Language: "json",
	})

	return 0
}

func (r *REST) check(L *lua.LState) int {
	code := L.CheckInt(1)
	tbl := L.CheckTable(2)

	if r.response == nil {
		L.RaiseError("send not called")
		return 0
	}

	if r.response.Code != code {
		L.RaiseError("expected response code %d, got %d\n%v", code, r.response.Code, r.response.Body.String())
		return 0
	}

	var res map[string]interface{}
	if err := json.Unmarshal(r.response.Body.Bytes(), &res); err != nil {
		L.RaiseError("unable to unmarshal response: %v", err)
		return 0
	}

	StdCheck(L, tbl, res)
	return 0
}

func (r *REST) addHeader(L *lua.LState) int {
	key := L.CheckString(1)
	value := L.CheckString(2)

	if r.headers == nil {
		r.headers = http.Header{}
	}

	r.headers.Add(key, value)

	return 0
}
