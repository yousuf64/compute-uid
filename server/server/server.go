package server

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yousuf64/shift"
	"log"
	"net/http"
	"sync/atomic"
	"unique-id-generator/server/uidgen"
	"unique-id-generator/server/virtualsvrplane"
)

const HeaderBucketId = "x-bucket-id"

type pvtShiftServer = *shift.Server

type Server struct {
	uidGen *uidgen.UIDGen
	vsp    *virtualsvrplane.VirtualSvrPlane
	logger *log.Logger

	pvtShiftServer
}

func New(uidGen *uidgen.UIDGen, vsp *virtualsvrplane.VirtualSvrPlane, logger *log.Logger) *Server {
	srv := &Server{uidGen: uidGen, vsp: vsp, logger: logger}

	var counter = &atomic.Int32{}

	r := shift.New()
	r.Use(ErrorHandler)
	r.GET("/generate-uid/:bucketId", srv.generateUidHandler)

	srv.pvtShiftServer = r.Serve()
	return srv
}

func (srv *Server) generateUidHandler(w http.ResponseWriter, r *http.Request, route shift.Route) error {
	headerBuckerId := r.Header.Get(HeaderBucketId)
	if headerBuckerId == "" {
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s expected in the header", HeaderBucketId))
	}

	bucketId := route.Params.Get("bucketId")
	if headerBuckerId != bucketId {
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("%s should match the :bucketId in the route", HeaderBucketId))
	}

	_, err := uuid.Parse(bucketId)
	if err != nil {
		srv.logger.Printf("failed to parse bucketId: %s\n", bucketId)
		return err
	}

	id, err := srv.vsp.ComputeId(bucketId)
	switch err {
	case nil:
		resp := UidResponse{Uid: id}
		srv.logger.Printf("replying %+v", resp)
		return srv.Reply200(w, resp)
	case uidgen.ErrMaxLimitReached:
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

type UidResponse struct {
	Uid uint64 `json:"uid"`
}
