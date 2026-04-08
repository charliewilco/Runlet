package runlet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type Server struct {
	runlet *Runlet
	router *gin.Engine
}

type createJobResponse struct {
	JobID  JobID  `json:"job_id"`
	Status string `json:"status"`
}

type eventHistoryResponse struct {
	JobID  JobID         `json:"job_id"`
	Events []EventRecord `json:"events"`
}

type cancelResponse struct {
	JobID  JobID  `json:"job_id"`
	Status string `json:"status"`
}

func NewServer(r *Runlet) *Server {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())

	server := &Server{
		runlet: r,
		router: router,
	}

	router.POST("/jobs", server.createJob)
	router.GET("/jobs/:id", server.getJob)
	router.GET("/jobs/:id/events", server.streamEvents)
	router.GET("/jobs/:id/events/history", server.eventHistory)
	router.POST("/jobs/:id/cancel", server.cancelJob)

	return server
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) Run(ctx context.Context, addr string) error {
	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)

		err := <-errCh
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("runlet: %w", err)
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("runlet: %w", err)
	}
}

func (s *Server) createJob(c *gin.Context) {
	var request JobRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.String(http.StatusBadRequest, "invalid request")
		return
	}

	jobID, err := s.runlet.CreateJob(c.Request.Context(), request)
	if err != nil {
		c.String(http.StatusInternalServerError, "error")
		return
	}

	c.JSON(http.StatusOK, createJobResponse{
		JobID:  jobID,
		Status: string(JobStatusQueued),
	})
}

func (s *Server) getJob(c *gin.Context) {
	job, err := s.runlet.Job(c.Request.Context(), JobID(c.Param("id")))
	if err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}

	c.JSON(http.StatusOK, job)
}

func (s *Server) eventHistory(c *gin.Context) {
	jobID := JobID(c.Param("id"))
	afterSeq, _ := strconv.ParseUint(c.DefaultQuery("after_seq", "0"), 10, 64)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "500"))
	if limit <= 0 {
		limit = 500
	}
	if limit > 5000 {
		limit = 5000
	}

	events, err := s.runlet.EventHistory(c.Request.Context(), jobID, afterSeq, limit)
	if err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}

	c.JSON(http.StatusOK, eventHistoryResponse{
		JobID:  jobID,
		Events: events,
	})
}

func (s *Server) streamEvents(c *gin.Context) {
	jobID := JobID(c.Param("id"))
	afterSeq := lastEventID(c.Request)

	job, err := s.runlet.Job(c.Request.Context(), jobID)
	if err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}

	events, unsubscribe, err := s.runlet.subscribe(jobID)
	if err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}
	defer unsubscribe()

	history, err := s.runlet.EventHistory(c.Request.Context(), jobID, afterSeq, s.runlet.config.MaxEventsPerJob)
	if err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.String(http.StatusInternalServerError, "stream unsupported")
		return
	}

	lastSent := afterSeq
	for _, event := range history {
		if event.Seq <= lastSent {
			continue
		}
		if err := writeSSE(c.Writer, event); err != nil {
			return
		}
		lastSent = event.Seq
		flusher.Flush()
		if event.Kind.terminal() {
			return
		}
	}

	if job.terminal() {
		return
	}

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Seq <= lastSent {
				continue
			}
			if err := writeSSE(c.Writer, event); err != nil {
				return
			}
			lastSent = event.Seq
			flusher.Flush()
			if event.Kind.terminal() {
				return
			}
		}
	}
}

func (s *Server) cancelJob(c *gin.Context) {
	jobID := JobID(c.Param("id"))
	if err := s.runlet.CancelJob(c.Request.Context(), jobID); err != nil {
		if errors.Is(err, errJobNotFound) {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.String(http.StatusInternalServerError, "error")
		return
	}

	c.JSON(http.StatusOK, cancelResponse{
		JobID:  jobID,
		Status: "canceling",
	})
}
