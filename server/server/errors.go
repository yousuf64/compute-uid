package server

import (
	"encoding/json"
	"github.com/yousuf64/shift"
	"net/http"
)

type ErrorResponse struct {
	Status       int    `json:"-"`
	ErrorMessage string `json:"error"`
}

func NewErrorResponse(status int, message string) ErrorResponse {
	return ErrorResponse{status, message}
}

func (e ErrorResponse) Error() string {
	return e.ErrorMessage
}

func ErrorHandler(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		err := next(w, r, route)
		if err != nil {
			errResp, ok := err.(ErrorResponse)
			if !ok {
				errResp.Status = http.StatusInternalServerError
				errResp.ErrorMessage = err.Error()
			}

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(errResp.Status)
			if encodeErr := json.NewEncoder(w).Encode(err); encodeErr != nil {
				_, _ = w.Write([]byte("error encoding failed"))
			}
		}
		return nil
	}
}
