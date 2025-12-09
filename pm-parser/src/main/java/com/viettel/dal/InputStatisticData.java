package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.viettel.common.Constant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InputStatisticData {

    @JsonAlias({"system_type", "type"})
    @SerializedName("system_type")
    private String systemType;

    @JsonAlias({"object_level"})
    @SerializedName("object_level")
    private String objectLevel;

    @JsonAlias({"summary_counter_kpi", "summary"})
    @SerializedName(value = "summary_counter_kpi", alternate = {"summary"})
    private List<String> listFieldSummary;

    @JsonProperty("peak_by_kpi_counter")
    @SerializedName("peak_by_kpi_counter")
    private Long peakTimeBy;

    @JsonProperty("enable_peak_interval")
    @SerializedName("enable_peak_interval")
    private Boolean peakTimeByType;

    @JsonProperty("is_absolute_time")
    @SerializedName("is_absolute_time")
    private Boolean absoluteTime;

    @JsonProperty("relative_time")
    @SerializedName("relative_time")
    private int relativeTime;

    @JsonProperty("from_time")
    @SerializedName("from_time")
    private String fromTime;

    @JsonProperty("to_time")
    @SerializedName("to_time")
    private String toTime;

    @JsonProperty("number_page")
    @SerializedName("number_page")
    private int numPage;

    @JsonProperty("page_size")
    @SerializedName("page_size")
    private int pageSize;

    private int offset;
    private int interval;

    @JsonProperty("interval_unit")
    @SerializedName("interval_unit")
    private Integer intervalUnit;

    @JsonProperty("ne_list_id")
    @SerializedName("ne_list_id")
    private List<Integer> listNE;

    @JsonProperty("ne_group_list_code")
    @SerializedName("ne_group_list_code")
    private List<String> listNEGroupCode;

    @JsonProperty("ne_group_condition_param")
    @SerializedName("ne_group_condition_param")
    private String NEGroupConditionParam;

    @JsonProperty("ne_group_condition_param_list")
    @SerializedName("ne_group_condition_param_list")
    private List<NEGroupConditionParamObject> NEGroupConditionParamList;

    @JsonProperty("list_counter_kpi")
    @SerializedName("list_counter_kpi")
    private List<Integer> listCounterKPI;

    @JsonProperty("kpi_counter_filters")
    @SerializedName("kpi_counter_filters")
    private List<EmsFilter> emsFilters;

    @JsonProperty("other_filters")
    @SerializedName("other_filters")
    private List<EmsFilter> otherFilters;

    @JsonProperty("range_time_filters")
    @SerializedName("range_time_filters")
    private List<TimeRange> ranges;

    @JsonProperty("show_recent_counter")
    @SerializedName("show_recent_counter")
    private boolean showRecentCounter;

    @JsonProperty("separate_time")
    @SerializedName("separate_time")
    private boolean separateTime;

    @JsonProperty("duration")
    @SerializedName("duration")
    private int duration;

    @JsonProperty("is_return_id")
    @SerializedName("is_return_id")
    private boolean isReturnCounterKpiId;

    @JsonProperty("timeRange")
    @SerializedName("timeRange")
    private TimeRangeRelative timeRange;


    @JsonProperty("rat_type_list")
    @SerializedName("rat_type_list")
    private List<String> ratTypeList;

    public void sortAllListInside() {
        if (listNE != null && !listNE.isEmpty()) {
            listNE.sort(Comparator.naturalOrder());
        }
        if (ranges != null && !ranges.isEmpty()) {
            ranges.sort(Comparator.comparing(TimeRange::getStart));
        }
    }

    public InputStatisticData clone() {
        InputStatisticData tmp = new InputStatisticData();
        tmp.setSystemType(this.systemType);
        tmp.setObjectLevel(this.objectLevel);
        tmp.setListFieldSummary(this.listFieldSummary);
        tmp.setPeakTimeBy(this.peakTimeBy);
        tmp.setPeakTimeByType(this.peakTimeByType);
        tmp.setAbsoluteTime(this.absoluteTime);

        if (this.relativeTime == 0 && this.timeRange != null) {
            tmp.setRelativeTime(this.timeRange.toMinute());
        } else {
            tmp.setRelativeTime(this.relativeTime);
        }

        tmp.setFromTime(this.fromTime);
        tmp.setToTime(this.toTime);
        tmp.setNumPage(this.numPage);
        tmp.setPageSize(this.pageSize);
        tmp.setOffset(this.offset);
        tmp.setInterval(this.interval);
        tmp.setIntervalUnit(this.intervalUnit == null ? Constant.IntervalUnit.MINUTE.getValue() : this.intervalUnit);
        tmp.setEmsFilters(this.emsFilters == null ? null :  new ArrayList<>(this.emsFilters));
        tmp.setOtherFilters(this.otherFilters == null ? null : new ArrayList<>(this.otherFilters) );
        tmp.setListCounterKPI(this.listCounterKPI == null ?  new ArrayList<>() : new ArrayList<>(this.listCounterKPI) );
        tmp.setRanges(this.ranges == null ? null : new ArrayList<>(this.ranges));
        tmp.setShowRecentCounter(this.showRecentCounter);
        tmp.setSeparateTime(this.separateTime);
        tmp.setListNE(this.listNE == null ? null : new ArrayList<>(this.listNE));
        tmp.setListNEGroupCode(this.listNEGroupCode);
        tmp.setNEGroupConditionParam(this.NEGroupConditionParam);
        tmp.setNEGroupConditionParamList(this.NEGroupConditionParamList == null ? null : new ArrayList<>(this.NEGroupConditionParamList));
        tmp.setDuration(this.duration);
        tmp.setReturnCounterKpiId(this.isReturnCounterKpiId);
        tmp.setTimeRange(this.timeRange);

        List<String> normalizedRatTypeList = new ArrayList<>();
        ratTypeList.forEach(ratType -> normalizedRatTypeList.add(ratType.toUpperCase().replace("-", "_")));
        tmp.setRatTypeList(normalizedRatTypeList);

        return tmp;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(this);
    }

    public String toJSONWithoutTime() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        InputStatisticData tmp = this.clone();
        tmp.setFromTime(null);
        tmp.setToTime(null);
        return gson.toJson(tmp);
    }
}
