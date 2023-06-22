package server

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yousuf64/shift"
	"log"
	"net/http"
	"sync/atomic"
	"unique-id-generator/server/compute"
	"unique-id-generator/server/computeplane"
)

const HeaderBucketId = "x-bucket-id"

type pvtShiftServer = *shift.Server

type Server struct {
	cp     *computeplane.ComputePlane
	logger *log.Logger

	pvtShiftServer
}

func New(cp *computeplane.ComputePlane, logger *log.Logger) *Server {
	srv := &Server{cp: cp, logger: logger}

	var counter = &atomic.Int32{}

	r := shift.New()
	r.Use(ErrorHandler)
	r.GET("/compute/:bucketId", srv.ComputeHandler)

	srv.pvtShiftServer = r.Serve()
	return srv
}

func (srv *Server) ComputeHandler(w http.ResponseWriter, r *http.Request, route shift.Route) error {
	headerBucketId := r.Header.Get(HeaderBucketId)
	if headerBucketId == "" {
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s expected in the header", HeaderBucketId))
	}

	bucketId := route.Params.Get("bucketId")
	if headerBucketId != bucketId {
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s should match the :bucketId in the route", HeaderBucketId))
	}

	_, err := uuid.Parse(bucketId)
	if err != nil {
		srv.logger.Printf("failed to parse bucketId: %s\n", bucketId)
		return err
	}

	id, err := srv.cp.ComputeId(bucketId)
	switch err {
	case nil:
		resp := Response{Id: id}
		srv.logger.Printf("replying %+v", resp)
		return srv.Reply200(w, resp)
	case compute.ErrMaxLimitReached:
		return NewErrorResponse(http.StatusConflict, "maximum reached")
	default:
		return err
	}
}

func (srv *Server) Reply200(w http.ResponseWriter, response any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Print("failed to encode the response")
		return err
	}
	return nil
}

type Response struct {
	Id uint64 `json:"id"`
}
