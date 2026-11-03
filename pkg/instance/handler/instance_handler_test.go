package instance_handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
)

// The service is never reached when the token does not own the path's instance, so a nil service proves it.
func advancedSettingsRequest(method string, tokenInstanceId string, pathInstanceId string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	h := &instanceHandler{}
	eng := gin.New()
	eng.Use(func(c *gin.Context) {
		c.Set("instance", &instance_model.Instance{Id: tokenInstanceId})
	})
	eng.GET("/instance/:instanceId/advanced-settings", h.GetAdvancedSettings)
	eng.PUT("/instance/:instanceId/advanced-settings", h.UpdateAdvancedSettings)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, "/instance/"+pathInstanceId+"/advanced-settings", nil)
	eng.ServeHTTP(rec, req)
	return rec
}

func TestAdvancedSettingsRejectsAnotherInstancesToken(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := advancedSettingsRequest(method, "instance-a", "instance-b")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s with another instance's token: got %d, want 403", method, rec.Code)
		}
	}
}

func TestOwnsInstance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name     string
		instance any
		pathId   string
		want     bool
	}{
		{"same instance", &instance_model.Instance{Id: "a"}, "a", true},
		{"other instance", &instance_model.Instance{Id: "a"}, "b", false},
		{"empty token instance id", &instance_model.Instance{Id: ""}, "a", false},
		{"nil instance", (*instance_model.Instance)(nil), "a", false},
		{"wrong type", "a", "a", false},
	}
	for _, tc := range cases {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Set("instance", tc.instance)
		if got := ownsInstance(c, tc.pathId); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
