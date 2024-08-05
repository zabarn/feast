package server

import (
	"bytes"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUnmarshalJSON(t *testing.T) {
	u := repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1, 2, 3]")))
	assert.Equal(t, []int64{1, 2, 3}, u.int64Val)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1.2, 2.3, 3.4]")))
	assert.Equal(t, []float64{1.2, 2.3, 3.4}, u.doubleVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[\"foo\", \"bar\"]")))
	assert.Equal(t, []string{"foo", "bar"}, u.stringVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[true, false, true]")))
	assert.Equal(t, []bool{true, false, true}, u.boolVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1, 2, 3], [4, 5, 6]]")))
	assert.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, u.int64ListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1.2, 2.3, 3.4], [10.2, 20.3, 30.4]]")))
	assert.Equal(t, [][]float64{{1.2, 2.3, 3.4}, {10.2, 20.3, 30.4}}, u.doubleListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[\"foo\", \"bar\"], [\"foo2\", \"bar2\"]]")))
	assert.Equal(t, [][]string{{"foo", "bar"}, {"foo2", "bar2"}}, u.stringListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[true, false, true], [false, true, false]]")))
	assert.Equal(t, [][]bool{{true, false, true}, {false, true, false}}, u.boolListVal)
}

func TestGetOnlineFeaturesWithValidRequest(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features: []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestGetOnlineFeaturesWithInvalidJSON(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	requestBody := []byte("invalid json")
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGetOnlineFeaturesWithEmptyFeatures(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features: []string{},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetOnlineFeaturesWithEmptyEntities(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features:         []string{"feature1", "feature2"},
		Entities:         map[string]repeatedValue{},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetOnlineFeaturesWithInvalidFeatureService(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	invalidFeatureService := "invalidFeatureService"
	request := getOnlineFeaturesRequest{
		FeatureService: &invalidFeatureService,
		Features:       []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGetOnlineFeaturesWithFeatureService(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	featureService := "testFeatureService"
	request := getOnlineFeaturesRequest{
		FeatureService: &featureService,
		Features:       []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestGetOnlineFeaturesWithoutFeatureService(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features: []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestGetOnlineFeaturesWithInvalidEntities(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features: []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"invalidEntity": {int64Val: []int64{1, 2, 3}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGetOnlineFeaturesWithEmptyRequestContext(t *testing.T) {
	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = &MockFeatureStore{}

	request := getOnlineFeaturesRequest{
		Features: []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		RequestContext:   map[string]repeatedValue{},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestGetOnlineFeaturesWithInvalidRequestContext(t *testing.T) {

	s := NewHttpServer(nil, nil)

	// Mocking the GetOnlineFeatures method
	s.fs = NewFeatureStore(getRepoConfig(), nil)

	request := getOnlineFeaturesRequest{
		Features: []string{"feature1", "feature2"},
		Entities: map[string]repeatedValue{
			"entity1": {int64Val: []int64{1, 2, 3}},
			"entity2": {stringVal: []string{"value1", "value2"}},
		},
		RequestContext: map[string]repeatedValue{
			"invalidContext": {int64Val: []int64{1, 2, 3}},
		},
		FullFeatureNames: true,
	}

	requestBody, _ := json.Marshal(request)
	req, _ := http.NewRequest("POST", "/get-online-features", bytes.NewBuffer(requestBody))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func getRepoConfig() (config registry.RepoConfig) {
	return registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type":              "redis",
			"connection_string": "localhost:6379",
		},
	}
}
