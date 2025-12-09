package com.viettel.schedule;

import com.viettel.common.Constant;
import com.viettel.dal.*;
import com.viettel.repository.CommonRepository;
import com.viettel.repository.CounterKpiRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.IndexedColors;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class ExcelUtil {

    private static final String NEW_LINE_SEPARATOR = "\n";
    private static final String COMMA_DELIMITER = ",";

    // private static final SimpleDateFormat sdf = new SimpleDateFormat(Constants.TIME_FORMAT);

    public static String renderExcel(
            ScheduleQuery otemplate,
            InputStatisticData inputStatisticData,
            List<ReportStatisticKPI> data,
            String exportFilePath,
            CounterKpiRepository counterKpiRepository,
            CommonRepository commonRepository
    ) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(Constant.TIME_FORMAT);
        if (inputStatisticData.getAbsoluteTime() != null && !inputStatisticData.getAbsoluteTime()) {
            Calendar calendar = Calendar.getInstance();
            inputStatisticData.setToTime(sdf.format(calendar.getTime()));
            calendar.add(Calendar.MINUTE, -inputStatisticData.getRelativeTime());
            inputStatisticData.setFromTime(sdf.format(calendar.getTime()));
        }

        Date startTime = sdf.parse(inputStatisticData.getFromTime());
        Date endTime = sdf.parse(inputStatisticData.getToTime());

        String strStartDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime);
        String title = "KPI Counter Report";
        String strEndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(endTime.getTime() - 1));

        if (strStartDate.equals(strEndDate)) {
            title += "\nTime: " + strStartDate;
        } else {
            title += "\nTime Range: " + strStartDate + " - " + strEndDate;
        }

        List<CounterKpi> listKPIObj = counterKpiRepository.getAllListKpiFomular(inputStatisticData.getListCounterKPI());
        if (inputStatisticData.isShowRecentCounter()) {
            Set<Integer> setKpiCounter = new LinkedHashSet<>(inputStatisticData.getListCounterKPI());
            for (CounterKpi item : listKPIObj) {
                CounterKpi obj = getCounterKPIObject(item.getId(), listKPIObj);
                if (obj.isKpi() && obj.getArrCounter() != null) {
                    // Parse chuỗi thành Set<Integer>
                    Set<Integer> parsedSet = new HashSet<>();
                    for (String s : obj.getArrCounter().split(",")) {
                        parsedSet.add(Integer.parseInt(s.trim()));
                    }
                    setKpiCounter.addAll(parsedSet);
                }
            }
            List<Integer> lstKpiCounter = new ArrayList<>(setKpiCounter);
            inputStatisticData.setListCounterKPI(lstKpiCounter);
        }

        listKPIObj = counterKpiRepository.getAllListKpiFomular(inputStatisticData.getListCounterKPI());
        // get KPI
        int index = 0;
        String[] kpiCounterName = new String[inputStatisticData.getListCounterKPI().size()];
        for (CounterKpi item : listKPIObj) {
            CounterKpi obj = getCounterKPIObject(item.getId(), listKPIObj);
            if (item.isKpi()) {
                kpiCounterName[index] = obj != null ? (obj.getName() + " (" + obj.getUnits() + ")") : "N/A";
            } else {
                kpiCounterName[index] = obj != null ? obj.getName() : "N/A";
            }
            index++;
        }

        // Extra Field
        String objectLevelName = inputStatisticData.getObjectLevel();
        List<ExtraFieldObject> lstAllCommonField = new ArrayList<>();
        HashMap<String, ExtraFieldObject> hmCommonFieldDB;

        lstAllCommonField.add(new ExtraFieldObject(1, Constants.DATE_TIME_KEY, Constants.DATE_TIME_VALUE, Constants.DATE_TIME_DISPLAY, "String", 1, 0, 1));
        lstAllCommonField.add(new ExtraFieldObject(2, Constants.DATE_KEY, Constants.DATE_VALUE, Constants.DATE_DISPLAY, "String", 1, 0, 1));
        lstAllCommonField.add(new ExtraFieldObject(3, Constants.TIME_KEY, Constants.TIME_VALUE, Constants.TIME_DISPLAY, "String", 1, 0, 1));
        lstAllCommonField.add(new ExtraFieldObject(4, Constants.NE_KEY, Constants.NE_VALUE, Constants.NE_DISPLAY, "String", 1, 0, 1));

        if (objectLevelName == null || objectLevelName.isEmpty()) {
            hmCommonFieldDB = commonRepository.getExtraField();
        } else {
            hmCommonFieldDB = commonRepository.getExtraField(objectLevelName);
        }

        if (hmCommonFieldDB != null && hmCommonFieldDB.size() > 0)
            hmCommonFieldDB.values().forEach(v -> lstAllCommonField.add(v));

        try {
            return exportKPIStatistic(otemplate, data, title, kpiCounterName, lstAllCommonField, endTime, "group_sum_no", exportFilePath);
        } catch (Exception e) {
            log.error("renderExcel() " + otemplate.getName() + ", title = " + title + ", err = " + e.getMessage(), e);
            return null;
        }
    }

    protected static CounterKpi getCounterKPIObject(int kpi_id, List<CounterKpi> dsSource) {
        for (CounterKpi o : dsSource) {
            if (o.getId() == kpi_id) return o;
        }
        return null;
    }

    public static String convertInterval(int interval, int unit) {
        String unitStr = convertUnit(unit);
        if (unit == 2) {
            if (60 == interval) {
                return "01hour";
            } else if (1440 == interval) {
                return "01day";
            }
        }

        // by default return by minute
        return String.format("%d%s", interval, unitStr);
    }

    private static String convertUnit(int unit) {
        switch (unit) {
            case 1:
                return "sec";
            case 3:
                return "hour";
            case 4:
                return "day";
            case 5:
                return "week";
            case 6:
                return "month";
            default:
                return "min";
        }
    }

    private static String exportKPIStatistic(
            ScheduleQuery otemplate,
            List<ReportStatisticKPI> data,
            String title,
            String[] kpiCounterName,
            List<ExtraFieldObject> lstAllCommonField,
            Date endTime,
            String summaryRptType,
            String exportFilePath
    ) throws Exception {
        String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String timeExport = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        HSSFRow exelRow = null;
        log.info("Receive_Mode = {}", otemplate.getStoreType());
        // if not configure export path, or send by email, then save to /tmp dir
        String path = (exportFilePath == null || Constants.SENDING_TYPE_EMAIL.equals(otemplate.getStoreType())) ?
                System.getProperty("java.io.tmpdir") + "/scheduleData" : exportFilePath + "/";
        // String path = System.getProperty("java.io.tmpdir") + "/scheduleData";
        String strToday = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(endTime);
        String filePath;

        File temp = new File(path);
        if (!temp.exists()) {
            temp.mkdir();
            path += today;
            temp = new File(path);
            temp.mkdir();
        } else {
            path += today;
            temp = new File(path);
            if (!temp.exists()) {
                temp.mkdir();
            }
        }

        String scheduleName = otemplate.getName().trim().replace(" ", "_").replace("&", "_");
        filePath = String.format("%s%sReport_%s_%s_%s.xls", path, File.separator, scheduleName,
                convertInterval(otemplate.getInputStatisticData().getInterval(), otemplate.getInputStatisticData().getIntervalUnit()), otemplate.getCreatedBy(), timeExport);

        File file = new File(filePath);
        FileOutputStream fileOut = new FileOutputStream(file);

        HSSFWorkbook workBook = new HSSFWorkbook();
        if (data.size() > 0) {
            //TODO:
            List<String> lstFieldSummary = new ArrayList<>();
            data.get(0).getExtraField().forEach((k, v) -> lstFieldSummary.add(k));
            lstAllCommonField.removeIf(item -> (!lstFieldSummary.contains(item.getColumnCode())));

            String[] extraFieldName = new String[lstAllCommonField.size()];
            String[] extraFieldNameDisplay = new String[lstAllCommonField.size()];
            int extraFieldIdx = 0;
            for (ExtraFieldObject item : lstAllCommonField) {
                extraFieldName[extraFieldIdx] = item.getColumnCode();
                extraFieldNameDisplay[extraFieldIdx] = item.getDisplayName();
                extraFieldIdx++;
            }

            HSSFSheet workSheet = createWorkSheetForKPIStatistic(workBook, title, extraFieldNameDisplay, kpiCounterName, summaryRptType);
            CreationHelper creationHelper = workBook.getCreationHelper();

            HSSFCellStyle cellStyle = formatCellStyle(workBook, "Times New Roman", (short) 11, HSSFFont.BOLDWEIGHT_NORMAL,
                    true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER);
            HSSFCellStyle cellStyleNeName = formatCellStyle(workBook, "Times New Roman", (short) 11,
                    HSSFFont.BOLDWEIGHT_NORMAL, true, HSSFCellStyle.ALIGN_LEFT, HSSFCellStyle.VERTICAL_CENTER);
            HSSFCellStyle cellStyle2 = formatCellStyle(workBook, "Times New Roman", (short) 11, HSSFFont.BOLDWEIGHT_NORMAL,
                    true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER);
            cellStyle2.setDataFormat(creationHelper.createDataFormat().getFormat("dd-mm-yyyy hh:mm:ss"));

            HSSFCellStyle cellStyleNumber = formatCellStyle(workBook, "Times New Roman", (short) 11,
                    HSSFFont.BOLDWEIGHT_NORMAL, true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER);
            cellStyleNumber.setDataFormat(HSSFDataFormat.getBuiltinFormat("0.00"));

            int colIdx = 0;
            // render data
            for (int i = 0; i < data.size(); i++) {
                ReportStatisticKPI rptRow;
                Map<String, Double> vals;
                Map<String, String> mExtraField;

                exelRow = workSheet.createRow((short) i + 3);
                rptRow = data.get(i);
                vals = rptRow.getParam();
                mExtraField = rptRow.getExtraField();
                // reset column index to zero, to begin new row
                colIdx = 0;

                // Extra field
                for (int idx = 0; idx < extraFieldName.length; idx++) {
                    HSSFCell oneCell;
                    oneCell = exelRow.createCell(colIdx);
                    try {
                        oneCell.setCellValue(mExtraField.getOrDefault(extraFieldName[idx], "N/A"));
                        setCellStyle(exelRow, colIdx, cellStyle);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                    colIdx++;
                }

                // KPI/Counter
                for (int idx = 0; idx < kpiCounterName.length; idx++) {
                    HSSFCell oneCell = null;
                    oneCell = exelRow.createCell(colIdx);
                    try {
                        if (vals.get(kpiCounterName[idx]) != null) {
                            oneCell.setCellValue(vals.get(kpiCounterName[idx]));
                        } else {
                            oneCell.setCellValue("N/A");
                        }
                        oneCell.setCellStyle(cellStyleNumber);
                    } catch (Exception e) {
                    }
                    colIdx++;
                }
            }
        } else {
            title += "\nNo data for the reporting period";
            createWorkSheetForKPIStatistic(workBook, title);
        }

        try {
            workBook.write(fileOut);
        } finally {
            fileOut.flush();
            fileOut.close();
            log.info("Create Excel Done");
        }

        return filePath;
    }

    private static Object getScheduleName(String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }
        String[] truncateList = new String[]{"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "'", "\"", ",", ".", "/", "\\", "[", "]", "?", ">", "<", "{", "}", "+", "|"};
        for (String ch : truncateList) {
            name = name.replace(ch, "");
        }
        return name.replace(" ", "-").trim();
    }

    private static Object getSummaryRpt(String summaryRptType) {
        if (summaryRptType == null || Constants.NORMAL_REPORT.equals(summaryRptType)) {
            return "";
        }
        return summaryRptType;
    }

    private static HSSFSheet createWorkSheetForKPIStatistic(
            HSSFWorkbook workBook,
            String title,
            String[] extraFieldName,
            String[] kpiCounterName,
            String summaryRptType
    ) {
        String[] nameFile = title.split("\n");
        HSSFCellStyle cellStyleColumnEncodeB = formatCellStyleColor(workBook, "Times New Roman", (short) 11,
                HSSFFont.BOLDWEIGHT_NORMAL, true, HSSFCellStyle.ALIGN_LEFT, HSSFCellStyle.VERTICAL_CENTER,
                IndexedColors.PALE_BLUE.getIndex());

        HSSFCellStyle cellStyleHeader = formatCellStyleColor(workBook, "Times New Roman", (short) 11,
                HSSFFont.BOLDWEIGHT_BOLD, true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER,
                IndexedColors.PALE_BLUE.getIndex());

        HSSFCellStyle titleStyle = formatCellStyleColor(workBook, "Times New Roman", (short) 13, HSSFFont.SS_SUPER,
                true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER, IndexedColors.LAVENDER.getIndex());
        HSSFSheet workSheet = workBook.createSheet(nameFile[0]);
        HSSFRow rowTitle = workSheet.createRow((short) 0);

        int kpiCounterNameSize = kpiCounterName != null ? kpiCounterName.length : 0;
        int extraFileNameSize = extraFieldName != null ? extraFieldName.length : 0;
        // set merge cell
        short mergedCellLength = (short) (extraFileNameSize + kpiCounterNameSize - 1);
        Region rg = new Region((short) 0, (short) 0, (short) 1, mergedCellLength);
        workSheet.addMergedRegion(rg);
        rowTitle.createCell((short) 0).setCellValue(title);
        setCellStyle(rowTitle, (short) 0, titleStyle);
        rowTitle.setHeight((short) 2000);
        rowTitle.getCell((short) 0).setCellStyle(cellStyleHeader);

        HSSFRow row = workSheet.createRow((short) 2);

        // column index
        int colIdx = 0;
        // extraFile Name
        for (int j = 0; j < extraFileNameSize; j++) {
            row.createCell(colIdx).setCellValue(extraFieldName[j]);
            setCellStyle(row, colIdx, cellStyleHeader);
            // next column
            colIdx++;
        }

        // kpiCounter Name
        for (int j = 0; j < kpiCounterNameSize; j++) {
            row.createCell(colIdx).setCellValue(kpiCounterName[j]);
            setCellStyle(row, colIdx, cellStyleHeader);
            // next column
            colIdx++;
        }

        //TODO: Set columnHeight
        row.setHeight((short) 1000);
        // workSheet.setColumnWidth(0, 5000);
        // workSheet.setColumnWidth(1, 5000);
        //TODO: Set ColumnWidth
        for (int j = 0; j < colIdx; j++) {
            workSheet.setColumnWidth(j, 5000);
        }
        return workSheet;
    }

    private static HSSFSheet createWorkSheetForKPIStatistic(
            HSSFWorkbook workBook,
            String title
    ) {
        String[] nameFile = title.split("\n");
        HSSFCellStyle cellStyleColumnEncodeB = formatCellStyleColor(workBook, "Times New Roman", (short) 11,
                HSSFFont.BOLDWEIGHT_NORMAL, true, HSSFCellStyle.ALIGN_LEFT, HSSFCellStyle.VERTICAL_CENTER,
                IndexedColors.PALE_BLUE.getIndex());

        HSSFCellStyle cellStyleHeader = formatCellStyleColor(workBook, "Times New Roman", (short) 11,
                HSSFFont.BOLDWEIGHT_BOLD, true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER,
                IndexedColors.PALE_BLUE.getIndex());

        HSSFCellStyle titleStyle = formatCellStyleColor(workBook, "Times New Roman", (short) 13, HSSFFont.SS_SUPER,
                true, HSSFCellStyle.ALIGN_CENTER, HSSFCellStyle.VERTICAL_CENTER, IndexedColors.LAVENDER.getIndex());
        HSSFSheet workSheet = workBook.createSheet(nameFile[0]);
        HSSFRow rowTitle = workSheet.createRow((short) 0);

        Region rg = new Region((short) 0, (short) 0, (short) 1, (short) 5);
        workSheet.addMergedRegion(rg);
        rowTitle.createCell((short) 0).setCellValue(title);
        setCellStyle(rowTitle, (short) 0, titleStyle);
        rowTitle.setHeight((short) 2000);
        rowTitle.getCell((short) 0).setCellStyle(cellStyleHeader);

        return workSheet;
    }

    private static HSSFCellStyle formatCellStyle(
            HSSFWorkbook workBook,
            String fontName,
            short fontSize,
            short fontBoldWeight,
            boolean wrapText,
            short fontAlignment,
            short fontVerticalAlignment
    ) {
        HSSFCellStyle cellStyle = workBook.createCellStyle();
        HSSFFont hSSFFont = workBook.createFont();
        hSSFFont.setFontName(fontName);
        hSSFFont.setFontHeightInPoints(fontSize);
        hSSFFont.setBoldweight(fontBoldWeight);
        cellStyle.setFont(hSSFFont);
        cellStyle.setWrapText(wrapText); // true
        cellStyle.setAlignment(fontAlignment);
        cellStyle.setVerticalAlignment(fontVerticalAlignment);
        cellStyle.setBorderBottom((short) 1);
        cellStyle.setBorderLeft((short) 1);
        cellStyle.setBorderRight((short) 1);
        cellStyle.setBorderTop((short) 1);

        return cellStyle;
    }

    private static HSSFCellStyle formatCellStyleColor(
            HSSFWorkbook workBook,
            String fontName,
            short fontSize,
            short fontBoldWeight,
            boolean wrapText,
            short fontAlignment,
            short fontVerticalAlignment,
            short codeColor
    ) {
        HSSFCellStyle cellStyle = workBook.createCellStyle();
        HSSFFont hSSFFont = workBook.createFont();
        hSSFFont.setFontName(fontName);
        hSSFFont.setFontHeightInPoints(fontSize);
        hSSFFont.setBoldweight(fontBoldWeight);
        cellStyle.setFont(hSSFFont);
        cellStyle.setWrapText(wrapText); // true
        cellStyle.setAlignment(fontAlignment);
        cellStyle.setVerticalAlignment(fontVerticalAlignment);
        cellStyle.setFillForegroundColor(codeColor);
        cellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
        cellStyle.setBorderBottom((short) 1);
        cellStyle.setBorderLeft((short) 1);
        cellStyle.setBorderRight((short) 1);
        cellStyle.setBorderTop((short) 1);

        return cellStyle;
    }

    private static void setCellStyle(HSSFRow row, int cellIndex, HSSFCellStyle cellStyle) {
        HSSFCell cell = row.getCell(cellIndex);
        cell.setCellStyle(cellStyle);
    }

    static class MyFileWriter extends OutputStreamWriter {
        public MyFileWriter(OutputStream arg0, String arg1) throws UnsupportedEncodingException {
            super(arg0, arg1);
        }

        public void appendColumn(String value) throws IOException {
            append(value);
            append(COMMA_DELIMITER);
        }

        public void insertNewLine() throws IOException {
            append(NEW_LINE_SEPARATOR);
        }
    }
}
