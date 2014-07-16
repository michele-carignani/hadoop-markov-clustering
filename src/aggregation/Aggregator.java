package aggregation;


import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Aggregator: can be used also for complex filters definition.
 *
 */
public class Aggregator {
	private HashSet<Integer> supportedWDays;
	private HashSet<Integer> supportedDays;
	private HashSet<Integer> supportedMonths;
	private HashSet<Integer> supportedHours;
	private String identifier = null;
	private int year = 0;
	private int seconds = 0;
	/**
	 * 
	 * @param s represents a complex criteria expression. Format:
	 * [[H|W|D|M]:(d+)[-(d+)|(,d+)*]?;]+
	 * [N:identifier of the criteria]+
	 * Where:
	 * - H,W,D,M identify, respectively, an hour, week day, day, month criteria
	 * - x-y represents the interval [x,y]
	 * - x represents the hour x
	 * - x,y,z represents the list of criteria x,y,z which could be either single hours or intervals 
	 * - N is a "special type" for giving the criteria a name. Notice that this is mandatory
	 * Conditions expressed on the same "normal type" (H,W,D,M) are in OR; those expressed on different "normal types" are in AND.
	 * A canonical form is in the format:
	 * H:[list of allowed hours]
	 * W:[list of allowed week days, issued using Calendar.$DAYOFTHEWEEK]
	 * D:[list of the allowed days]
	 * M:[list of the allowed months]
	 * Y:aaaa (no list allowed)
	 * N:String identifying the criteria
	 * @param aggregPeriod the aggregation period for translating hours back.
	 * @throws IllegalArgumentException in case the name for the criteria is not supplied or a criteria has an invalid format
	 */
	public Aggregator(String s) throws IllegalArgumentException{
		supportedWDays = new HashSet<Integer>();
		supportedDays = new HashSet<Integer>();
		supportedMonths = new HashSet<Integer>();
		supportedHours = new HashSet<Integer>();
		String[] criteria = s.split(";");
		for (String c: criteria) parseCriteria(c);
		if (identifier == null) throw new IllegalArgumentException("Please provide a name for this aggregator");
		if (supportedMonths.size() == 0) critIntervals(Calendar.JANUARY+"-"+Calendar.DECEMBER, supportedMonths);
		if (supportedDays.size() == 0)  critIntervals("1-31", supportedDays);
		if (supportedWDays.size() == 0) critIntervals(Calendar.SUNDAY+"-"+Calendar.SATURDAY, supportedWDays);
		if (supportedHours.size() == 0) critIntervals("0-23", supportedHours);
		/** Calculate total seconds in the aggregation */
		seconds = 3600 * supportedHours.size() * calculateValidDays();
	}

	private int calculateValidDays() {
		int totalDays = 0;
		Set<List<Integer>> allDays = Sets.cartesianProduct(ImmutableList.of(ImmutableSet.copyOf(supportedDays), ImmutableSet.copyOf(supportedMonths)));
		
		for (List<Integer> day: allDays) {
			try {
				Calendar cal = Calendar.getInstance();
				cal.setLenient(false);
				cal.set(year, day.get(1), day.get(0));
				int day_of_week = cal.get(Calendar.DAY_OF_WEEK);		
				for (int weekday: supportedWDays)
					if (weekday == day_of_week) totalDays++;
			} catch (ArrayIndexOutOfBoundsException exc) {
				//do nothing. Just go on and skip
			} catch (IllegalArgumentException illegal) {
				//do nothing also, this means that the date is not valid.
				//throw new IllegalArgumentException("WTF, i was trying with "+day.get(1)+","+day.get(0));
			}
		}
		return totalDays;
	}


	private void critIntervals(String string, Set<Integer> l) {
		if (string.contains(",")) { /**	List of accepted values */
			String[] singletons = string.split(",");
			for(String s: singletons) critIntervals(s, l);
			return;
		}
		if (string.contains("-")) { /** Interval */
			String[] interval = string.split("-");
			for (int i = Integer.parseInt(interval[0]); i <= Integer.parseInt(interval[1]); i++) {
				l.add(i);
			}
			return;
		}
		/** Singleton */
		l.add(Integer.parseInt(string));
	}

	private void parseCriteria(String criteria) {
		String[] record = criteria.split(":");
		switch(record[0]) {
		case "H": critIntervals(record[1], supportedHours);  return;
		case "W": critIntervals(record[1], supportedWDays); return;
		case "D": critIntervals(record[1], supportedDays); return;
		case "M": critIntervals(record[1], supportedMonths); return;
		case "Y": year = Integer.parseInt(record[1]); return;
		case "N": identifier = record[1]; return;
		default: throw new IllegalArgumentException("The string \""+criteria+"\" is not a valid criteria");
		}
	}	

	public static boolean checkCriteria(int[] criteria, int value) {
		return (criteria.length == 1) ? criteria[0] == value : criteria[0] <= value && criteria[1] >= value;
	}


	public boolean respects(String key) {
		// gets day-month-year-hour
		String[] fields = key.split("-");
		int day = Integer.parseInt(fields[0]);
		int month = Integer.parseInt(fields[1]);
		int year = Integer.parseInt(fields[2]);
		int hour = Integer.parseInt(fields[3]);
		boolean respected = false;
		/** Check criteria from most restrictive to least restrictive */
		for (int allowedMonths: supportedMonths) {
			respected = respected || allowedMonths == month; 
		}
		if (!respected && supportedMonths.size() > 0) return false;
		respected = false;
		for (int allowedWeekDay: supportedWDays) {
			Calendar c = Calendar.getInstance();
			c.set(year, month, day);
			int day_of_week = c.get(Calendar.DAY_OF_WEEK);
			respected = respected || allowedWeekDay == day_of_week;
		}
		if (!respected && supportedWDays.size() > 0) return false;
		respected = false;
		for(int allowedDays : supportedDays) {
			respected = respected || allowedDays == day;
		}
		if (!respected && supportedDays.size() > 0) return false;
		respected = false;
		for (int allowedHours : supportedHours) {
			respected = respected || allowedHours == hour;
		}
		return true;

	}

	public String getIdentifier() {
		return identifier;
	}
	
	public int getAggregationLength(int unitSeconds) {
		return getTotalSeconds()/unitSeconds;
	}

	public int getTotalSeconds() {
		return seconds;
	}
}