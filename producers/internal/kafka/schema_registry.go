package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type SchemaRegistryClient struct {
	BaseURL string
}

// RegisterSchema POSTs the schema to Confluent Schema Registry and returns the Schema ID
func (sr *SchemaRegistryClient) RegisterSchema(subject, schemaStr string) (int, error) {
	payload := map[string]string{"schema": schemaStr}
	data, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/subjects/%s/versions", sr.BaseURL, subject)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	json.Unmarshal(body, &result)
	return result.ID, nil
}
