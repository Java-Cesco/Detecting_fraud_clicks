import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Calendar 객체 관련 기능들을 모아놓은 유틸리티 클래스
 *
 * @author croute
 * @since 2011.02.10
 */
public class DateUtil
{

    /**
     * 캘린더 객체를 yyyy-MM-dd HH:mm:ss 형태의 문자열로 변환합니다.
     *
     * @param cal 캘린더 객체
     * @return 변환된 문자열
     */
    public static String StringFromCalendar(Calendar cal)
    {
        // 날짜를 통신용 문자열로 변경
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(cal.getTime());
    }

    /**
     * 캘린더 객체를 yyyy-MM-dd형태의 문자열로 변환합니다.
     *
     * @param cal 캘린더 객체
     * @return 변환된 문자열
     */
    public static String StringSimpleFromCalendar(Calendar cal)
    {
        // 날짜를 통신용 문자열로 변경
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(cal.getTime());
    }

    /**
     * yyyy-MM-dd HH:mm:ss 형태의 문자열을 캘린더 객체로 변환합니다.
     * 만약 변환에 실패할 경우 오늘 날짜를 반환합니다.
     *
     * @param date 날짜를 나타내는 문자열
     * @return 변환된 캘린더 객체
     */
    public static Calendar CalendarFromString(String date)
    {
        if (date.length() == 0)
            return  null;
        Calendar cal = Calendar.getInstance();
        try
        {
            //String oldstring = "2011-01-18 00:00:00.0";
            // Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(oldstring);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            cal.setTime(formatter.parse(date));
        }
        catch(ParseException e)
        {
            e.printStackTrace();
        }
        return cal;
    }

    /**
     * yyyy-MM-dd 형태의 문자열을 캘린더 객체로 변환합니다.
     * 만약 변환에 실패할 경우 오늘 날짜를 반환합니다.
     *
     * @param date 날짜를 나타내는 문자열
     * @return 변환된 캘린더 객체
     */
    public static Calendar CalendarFromStringSimple(String date)
    {
        Calendar cal = Calendar.getInstance();

        try
        {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            cal.setTime(formatter.parse(date));
        }
        catch(ParseException e)
        {
            e.printStackTrace();
        }
        return cal;
    }
}