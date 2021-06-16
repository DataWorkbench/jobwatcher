package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/jobwpb"
	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/DataWorkbench/jobwatcher/executor"
)

type JobWatcherServer struct {
	jobwpb.UnimplementedJobwatcherServer
	executor   *executor.JobwatcherExecutor
	emptyReply *model.EmptyStruct
}

func NewJobWatcherServer(executor *executor.JobwatcherExecutor) *JobWatcherServer {
	return &JobWatcherServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

func (s *JobWatcherServer) WatchJob(ctx context.Context, req *jobwpb.WatchJobRequest) (*model.EmptyStruct, error) {
	err := s.executor.WatchJob(ctx, req.JobInfo)
	return s.emptyReply, err
}
