#pragma once

int64		ScheduleCronJob(char *jobName, char *schedule, char *command);
char	   *GetCronJobSchedule(char *jobName, char *command);
char	   *GetCronJobCommand(char *jobName);
void		UnscheduleCronJob(char *jobName);
