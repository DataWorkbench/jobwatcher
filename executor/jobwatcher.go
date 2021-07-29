package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/functions"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobpb"

	"gorm.io/gorm"
)

type jobQueueType struct {
	Job             functions.JobWatchInfo
	ServerAddr      string
	RunEnd          bool
	ParagraphIndex  int
	ParagraphID     string
	StatusFailedNum int32
	HttpClient      functions.HttpClient
}

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

func GetNextParagraphIDValid(job jobQueueType) (r jobQueueType) {
Retry:
	r = GetNextParagraphID(job)
	if r.ParagraphID == "" && r.RunEnd == false {
		job = r
		goto Retry
	}
	return
}

func GetNextParagraphID(job jobQueueType) (r jobQueueType) {
	order := make(map[int]string)
	order[0] = job.Job.FlinkParagraphIDs.Conf
	order[1] = job.Job.FlinkParagraphIDs.Depends
	order[2] = job.Job.FlinkParagraphIDs.FuncScala
	order[3] = job.Job.FlinkParagraphIDs.MainRun

	r = job

	if job.ParagraphIndex == len(order)+1 {
		r.RunEnd = true
	} else {
		r.ParagraphIndex = job.ParagraphIndex + 1
		r.ParagraphID = order[r.ParagraphIndex]
	}
	return
}

func InitJobInfo(watchInfo functions.JobWatchInfo) (job jobQueueType) {
	job.Job = watchInfo
	job.ParagraphIndex = 0
	job.ParagraphID = watchInfo.FlinkParagraphIDs.Conf
	job.RunEnd = false
	job.StatusFailedNum = 0
	job.ServerAddr = watchInfo.ServerAddr
	job.HttpClient = functions.NewHttpClient(watchInfo.ServerAddr)

	return
}

func (ex *JobwatcherExecutor) WatchJobThread(ctx context.Context) {
	var (
		err    error
		status string
	)

	jobQueue := make(map[string]jobQueueType)

	for true {
		select {
		case info := <-ex.watchChan:
			jobQueue[info.ID] = InitJobInfo(info)
			if err = functions.ModifyStatus(ctx, info.ID, constants.StatusRunning, "ready to checkstatus", info.FlinkResources, info.EngineType, ex.db, ex.logger, jobQueue[info.ID].HttpClient, ex.jobDevClient); err != nil {
				ex.logger.Error().Msg("can't change the job status to  running").String("jobid", info.ID).Fire()
			}
		case <-time.After(time.Second * 1):
			for id, job := range jobQueue {
				for true {
					if status, err = job.HttpClient.GetParagraphStatus(job.Job.NoteID, job.ParagraphID); err != nil {
						job.StatusFailedNum += 1
						jobQueue[id] = job
						ex.logger.Error().Msg("can't get this paragraph status").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).Int32("failednum", job.StatusFailedNum).Fire()

						if job.StatusFailedNum < functions.MaxStatusFailedNum {
							break
						} else {
							status = functions.ParagraphError
							err = nil
						}
					}
					if status == functions.ParagraphFinish {
						job = GetNextParagraphIDValid(job)
						if job.RunEnd == true {
							var jobmsg string

							if jobmsg, err = job.HttpClient.GetParagraphResultOutput(job.Job.NoteID, job.ParagraphID); err != nil {
								ex.logger.Error().Msg("can't get this paragraph info for a error paragraph").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).String("error msg", err.Error()).Fire()
								jobmsg = "get error message failed"
							}
							if err = functions.ModifyStatus(ctx, job.Job.ID, constants.StatusFinish, jobmsg, job.Job.FlinkResources, job.Job.EngineType, ex.db, ex.logger, job.HttpClient, ex.jobDevClient); err != nil {
								ex.logger.Error().Msg("can't change the job status to finish").String("jobid", job.Job.ID).Fire()
								break
							}

							if err = job.HttpClient.DeleteNote(job.Job.NoteID); err != nil {
								ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
							}
							delete(jobQueue, id)
							break
						}
						jobQueue[id] = job
						if err = functions.ModifyStatus(ctx, job.Job.ID, constants.StatusRunning, job.ParagraphID+" is running", job.Job.FlinkResources, job.Job.EngineType, ex.db, ex.logger, job.HttpClient, ex.jobDevClient); err != nil {
							ex.logger.Error().Msg("can't change the job status to running").String("jobid", job.Job.ID).String("paragraphid", job.ParagraphID).Fire()
							break
						}
					} else if status == functions.ParagraphError {
						var jobmsg string

						if jobmsg, err = job.HttpClient.GetParagraphResultOutput(job.Job.NoteID, job.ParagraphID); err != nil {
							ex.logger.Error().Msg("can't get this paragraph info for a error paragraph").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).String("error msg", err.Error()).Fire()
							jobmsg = "get error message failed"
						}

						if err = functions.ModifyStatus(ctx, job.Job.ID, constants.StatusFailed, jobmsg, job.Job.FlinkResources, job.Job.EngineType, ex.db, ex.logger, job.HttpClient, ex.jobDevClient); err != nil {
							ex.logger.Error().Msg("can't change the job status to failed").String("jobid", job.Job.ID).Fire()
							//break
						}
						if job.StatusFailedNum < functions.MaxStatusFailedNum {
							if err = job.HttpClient.DeleteNote(job.Job.NoteID); err != nil {
								ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
							}
						} else {
							ex.logger.Warn().Msg("don't delete the job note, please check the failed resaon.").String("jobid", job.Job.ID).Fire()
						}
						delete(jobQueue, id)
						break
					} else if status == functions.ParagraphAbort {
						var jobmsg string

						if jobmsg, err = job.HttpClient.GetParagraphResultOutput(job.Job.NoteID, job.ParagraphID); err != nil {
							ex.logger.Error().Msg("can't get this paragraph info for a error paragraph").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).String("error msg", err.Error()).Fire()
							jobmsg = "get error message failed"
						}

						if err = functions.ModifyStatus(ctx, job.Job.ID, constants.StatusFinish, jobmsg, job.Job.FlinkResources, job.Job.EngineType, ex.db, ex.logger, job.HttpClient, ex.jobDevClient); err != nil {
							ex.logger.Error().Msg("can't change the job status to finish(abort)").String("jobid", job.Job.ID).Fire()
							break
						}

						if err = job.HttpClient.DeleteNote(job.Job.NoteID); err != nil {
							ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
						}
						delete(jobQueue, id)
						break
					} else {
						/* paragraph is running
						ParagraphUnknown = "UNKNOWN"
						ParagraphRunning = "RUNNING"
						ParagraphReady = "READY"
						ParagraphPending = "PENDING"
						*/
						break
					}
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
	if err = db.Table(functions.JobmanagerTableName).Select("id, noteid, paragraph,resources,enginetype").Where("status = '" + constants.StatusRunningString + "'").Scan(&jobs).Error; err != nil {
		ex.logger.Error().Msg("can't scan jobmanager table for pickup alone job").Fire()
		return
	}

	for _, job := range jobs {
		var watchInfo functions.JobWatchInfo
		var Pa constants.FlinkParagraphsInfo
		var r constants.JobResources

		watchInfo.ID = job.ID
		watchInfo.NoteID = job.NoteID
		if err = json.Unmarshal([]byte(job.Paragraph), &Pa); err != nil {
			return
		}
		watchInfo.FlinkParagraphIDs = Pa
		if err = json.Unmarshal([]byte(job.Resources), &r); err != nil {
			return
		}
		watchInfo.FlinkResources = r
		watchInfo.EngineType = job.EngineType
		ex.logger.Info().Msg("pickup alone job").String("jobid", job.ID).Fire()

		ex.watchChan <- watchInfo
	}

	return
}

func (ex *JobwatcherExecutor) GetJobStatus(ctx context.Context, ID string) (rep jobpb.JobReply, err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)

	if tmperr != nil {
		err = tmperr
		return
	}

	rep.State = functions.StringStatusToInt32(job.Status)
	rep.Message = job.Message
	return
}

func (ex *JobwatcherExecutor) GetJobInfo(ctx context.Context, ID string) (job functions.JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(functions.JobmanagerTableName).Select("noteid, status,message,enginetype").Where("id = '" + ID + "'").Scan(&job).Error
	return
}
