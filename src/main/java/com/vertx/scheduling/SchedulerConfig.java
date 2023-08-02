package com.vertx.scheduling;

import com.lsmdb.service.LSMService;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

@Slf4j
public class SchedulerConfig {

  private Scheduler sc;

  public SchedulerConfig() throws SchedulerException {
    commenceSchedule();
  }

  public void scheduleMergeSegments(final JsonObject config, final LSMService lsmService) throws SchedulerException {
    var mergeScheduleConfig = config.getJsonObject("mergeSchedule");
    final var cron = cronFound(mergeScheduleConfig) ? mergeScheduleConfig.getString("cron") : "0 */1 * * * ?";
    final var trigger = getTrigger(cron);
    final var job = createJobDetail(lsmService);
    sc.scheduleJob(job, trigger);
    log.info("Merge segments with cron - {}", cron);
  }

  private boolean cronFound(JsonObject mergeScheduleConfig) {
    return mergeScheduleConfig != null && mergeScheduleConfig.getString("cron") != null;
  }

  private JobDetail createJobDetail(final LSMService lsmService) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put("lsmService", lsmService);

    return JobBuilder.newJob(ScheduledMerge.class)
        .usingJobData(dataMap)
        .withIdentity("Merge Segments", "1")
        .requestRecovery(false)
        .build();
  }

  private void commenceSchedule() throws SchedulerException {
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    sc = schedulerFactory.getScheduler();
    sc.start();
  }

  private CronTrigger getTrigger(final String cron) {
    return TriggerBuilder.newTrigger()
        .withIdentity("Merge Segments Trigger", "1")
        .withSchedule(CronScheduleBuilder.cronSchedule(cron))
        .build();
  }
}
