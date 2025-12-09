package com.viettel.schedule;

import com.viettel.dal.InputStatisticData;
import com.viettel.dal.ReportStatisticKPI;
import com.viettel.repository.CommonRepository;
import com.viettel.repository.CounterKpiRepository;
import com.viettel.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

@Slf4j
@Component
public class ScheduleTask {

    @Value("${spring.haipAddress.host}")
    private String IPHA;

    @Value("${spring.mail.footer}")
    private String footer;

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
    private final SimpleDateFormat sdfDetail = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private Util util;

    @Autowired
    private CommonRepository commonRepository;

    @Autowired
    private CounterKpiRepository counterKpiRepository;

    @Autowired
    private EmailSender emailSender;

    @Value("${is-hdcaching-ready}")
    private boolean isHDcachingReady;

    public void runSchedule(ScheduleQuery oq) throws ParseException, InterruptedException {
        // Query
        String email_content = "";
        String email_title = "";
        String newPath = "";

        String exportFilePath = oq.getExportFilePath();
        InputStatisticData inputStatisticData = oq.getInputStatisticData().clone();

        if (inputStatisticData.getPeakTimeBy() != null && inputStatisticData.getPeakTimeBy() == 0) {
            inputStatisticData.setPeakTimeBy(null);
        }

        if (inputStatisticData.getAbsoluteTime() != null && !inputStatisticData.getAbsoluteTime()) {
            Calendar dtNow = getTimeNowByInterval(inputStatisticData.getInterval(), inputStatisticData.getIntervalUnit());

            // TODO: Set toDatetime
            inputStatisticData.setToTime(sdfDetail.format(dtNow.getTime()));
            // TODO: Set fromDatetime
            dtNow.add(Calendar.MINUTE, -inputStatisticData.getRelativeTime());
            inputStatisticData.setFromTime(sdfDetail.format(dtNow.getTime()));
            inputStatisticData.setAbsoluteTime(true);
            inputStatisticData.setRelativeTime(0);
        }

        List<ReportStatisticKPI> result = util.getStatisticFromDatabase(inputStatisticData.clone(), isHDcachingReady);

        if (result.size() > 0) {
            newPath = ExcelUtil.renderExcel(oq, inputStatisticData, result, exportFilePath, counterKpiRepository, commonRepository);
            log.info("Schedule task: Sending email " + newPath);

            email_title = String.format(Constants.SCHEDULE_RPT_DAILY_TITLE_VN, oq.getName(), sdf.format(Calendar.getInstance().getTime()));

            // TODO: fix bug
            String periodString = ExcelUtil.convertInterval(inputStatisticData.getInterval(), inputStatisticData.getIntervalUnit());

            email_content = String.format(Constants.SCHEDULE_RPT_CONTENT_EN,
                    oq.getName(),
                    inputStatisticData.getFromTime(),
                    inputStatisticData.getToTime(),
                    periodString,
                    footer);

            // Check store or send by email
            if (oq.getStoreType().equals(Constants.SENDING_TYPE_EMAIL) || Constants.SENDING_TYPE_BOTH.equals(oq.getStoreType())) {
                try {
                    log.info("NewPath: {}", newPath);
                    emailSender.sendManyPeopleAttachment(parseInformList(oq.getInformList()), email_title, email_content, newPath);
                } catch (Exception e) {
                    log.error("Schedule task: error sending email ", e);
                }
            }

            // Delete temp file
            if (newPath != null && Constants.SENDING_TYPE_EMAIL.equals(oq.getStoreType())) {
                if (!new File(newPath).delete()) {
                    log.info("delete export file failed");
                } else {
                    log.info("delete export file");
                }
            } else if (oq.getStoreType().equals(Constants.SENDING_NONE)) {
                if (newPath != null) {
                    if (!new File(newPath).delete()) {
                        log.info("delete export file failed");
                    } else {
                        log.info("delete export file");
                    }
                }
            }
        } else {
            log.info("Report's data null.");
        }
    }

    private Calendar getTimeNowByInterval(int interval, int intervalUnit) {
        Calendar cal = new GregorianCalendar(); // thời gian hiện tại

        switch (intervalUnit) {
            case 1: // giây
                int second = cal.get(Calendar.SECOND);
                cal.set(Calendar.SECOND, (second / interval) * interval);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case 2: // phút
                int minute = cal.get(Calendar.MINUTE);
                cal.set(Calendar.MINUTE, (minute / interval) * interval);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case 3: // giờ
                int hour = cal.get(Calendar.HOUR_OF_DAY);
                cal.set(Calendar.HOUR_OF_DAY, (hour / interval) * interval);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case 4: // ngày
                int day = cal.get(Calendar.DAY_OF_MONTH);
                cal.set(Calendar.DAY_OF_MONTH, ((day - 1) / interval) * interval + 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case 5: // tuần
                int week = cal.get(Calendar.WEEK_OF_YEAR);
                cal.set(Calendar.WEEK_OF_YEAR, ((week - 1) / interval) * interval + 1);
                cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case 6: // tháng
                int month = cal.get(Calendar.MONTH);
                cal.set(Calendar.MONTH, (month / interval) * interval);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            default:
                throw new IllegalArgumentException("intervalUnit is not valid");
        }
        return cal;
    }

    private List<String> parseInformList(String informList) {
        if (informList == null || informList.trim().isEmpty()) {
            return List.of();
        }
        return List.of(informList.split(";"));
    }
}
