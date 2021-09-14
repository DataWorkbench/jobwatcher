package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/functions"
	"github.com/DataWorkbench/glog"

	"gorm.io/gorm"
)

type JobwatcherExecutor struct {
	db           *gorm.DB
	watchChan    chan functions.JobWatchInfo
	jobDevClient functions.JobdevClient
	ctx          context.Context
	logger       *glog.Logger
}

func NewJobWatcherExecutor(db *gorm.DB, jobwork int32, ictx context.Context, logger *glog.Logger, PickupAloneJob int32, jClient functions.JobdevClient) *JobwatcherExecutor {
	ex := &JobwatcherExecutor{
		db:           db,
		watchChan:    make(chan functions.JobWatchInfo, jobwork),
		jobDevClient: jClient,
		ctx:          ictx,
		logger:       logger,
	}

	for i := int32(0); i < jobwork; i++ {
		go ex.WatchJobThread(ex.ctx)
	}

	if PickupAloneJob != 0 {
		ex.PickupAloneJobs(ex.ctx)
	}
	return ex
}

func (ex *JobwatcherExecutor) WatchJob(ctx context.Context, jobInfo string) (err error) {
	var watchInfo functions.JobWatchInfo

	if err = json.Unmarshal([]byte(jobInfo), &watchInfo); err != nil {
		return
	}

	ex.watchChan <- watchInfo
	return
}

func (ex *JobwatcherExecutor) WatchJobThread(ctx context.Context) {
	jobQueue := make(map[string]functions.JobQueueType)

	for true {
		select {
		case info := <-ex.watchChan:
			jobQueue[info.JobID] = functions.InitJobInfo(info)
		case <-time.After(time.Second * 1):
			for id, job := range jobQueue {
				retjob, _ := functions.GetZeppelinJobState(ctx, job, ex.logger, ex.db, ex.jobDevClient)
				if retjob.Watch.JobState.State != int32(constants.StatusRunning) {
					delete(jobQueue, id)
				}
			}
		}
	}
}

func (ex *JobwatcherExecutor) PickupAloneJobs(ctx context.Context) {
	var (
		err  error
		jobs []functions.JobmanagerInfo
	)

	db := ex.db.WithContext(ctx)
	if err = db.Table(functions.JobTableName).Select("*").Where("status = '" + constants.StatusRunningString + "'").Scan(&jobs).Error; err != nil {
		ex.logger.Error().Msg("can't scan jobmanager table for pickup alone job").Fire()
		return
	}

	for _, job := range jobs {
		watchInfo := functions.JobInfoToWatchInfo(job)
		ex.logger.Info().Msg("pickup alone job").String("jobid", watchInfo.JobID).Fire()

		ex.watchChan <- watchInfo
	}

	return
}
