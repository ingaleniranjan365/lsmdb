package com.vertx.scheduling;

import com.lsmdb.service.LSMService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

@Slf4j
@DisallowConcurrentExecution
public class ScheduledMerge implements Job {

  @SneakyThrows
  @Override
  public void execute(final JobExecutionContext context) {
    LSMService lsmService = (LSMService) context.getJobDetail().getJobDataMap().get("lsmService");
    lsmService.merge();
  }

}
