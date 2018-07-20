package bench

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	client := NewHttpClient("http://localhost:9000")
	b.ResetTimer()
	b.N = 2000000
	for i := 0; i < b.N; i++ {
		err := client.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		if err != nil {
			b.Fatalf("Failed for index %d: %s", i, err)
		}
	}
}

func NewHttpClient(endpoint string) *HttpClient {
	return &HttpClient{
		endpoint,
		*http.DefaultClient,
	}
}

type HttpClient struct {
	endpoint string
	http.Client
}

func (c HttpClient) doReq(method, path string, data []byte) ([]byte, error) {

	req, err := http.NewRequest(method, c.endpoint+path, bytes.NewBuffer(data))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("Unexpected server error")
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, fmt.Errorf("Invalid request")
	}

	return bodyBytes, nil

}

func (c HttpClient) Set(key, value string) error {
	data := []byte(fmt.Sprintf(`{"%s": "%s"}`, key, value))
	_, err := c.doReq("POST", "/key", data)
	if err != nil {
		return err
	}
	return nil
}
