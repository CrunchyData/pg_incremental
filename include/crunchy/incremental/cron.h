#pragma once

int64		ScheduleCronJob(char *jobName, char *schedule, char *command);
char	   *GetCronJobSchedule(char *jobName, char *command);
void		UnscheduleCronJob(char *jobName);
