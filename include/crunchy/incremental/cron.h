#pragma once

int64		ScheduleCronJob(char *jobName, char *schedule, char *command);
char	   *GetCronJobSchedule(char *jobName);
void		UnscheduleCronJob(char *jobName);
