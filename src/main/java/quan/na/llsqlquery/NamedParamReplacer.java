package quan.na.llsqlquery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NamedParamReplacer {
	private String preparedStatement;
	private List<Object> parameters;

	public NamedParamReplacer(String sql, Map<String, Object> params, boolean acceptNulls) {
		// Parse sql and create preparing statement and its parameter list
		StringBuilder sqlBuilder = new StringBuilder();
		List<Object> paramList = new ArrayList<>();
		int curPos = 0;
		int colonPos;
		do {
			colonPos = sql.indexOf(':', curPos);
			if (colonPos == -1) {
				// Append from curPos -> end
				sqlBuilder.append(sql.substring(curPos, sql.length()));
			} else {
				// Calculate the end of parameter name
			    int endNamePos = -1;
			    for (char specChar : Arrays.asList(' ', ',', ')', '(', '|', '=', '+', '-', ';')) {
			        int specPos = sql.indexOf(specChar, colonPos);
			        if (specPos != -1) {
			            if (endNamePos == -1)
			                endNamePos = specPos;
			            else if (endNamePos > specPos)
			                endNamePos = specPos;
			        }
			    }
				if (-1 == endNamePos)
					endNamePos = sql.length();
				// Get parameter from provided map
				String paramName = sql.substring(colonPos+1, endNamePos);
				if (null == params.get(paramName) && !acceptNulls)
					throw new IllegalStateException("Can not find parameter for " + paramName);
				paramList.add(params.get(paramName));
				// Append SQL with ?
				sqlBuilder.append(sql.substring(curPos, colonPos));
				sqlBuilder.append("?");
				curPos = endNamePos;
			}
		} while (colonPos != -1 && curPos < sql.length());
		preparedStatement = sqlBuilder.toString();
		parameters = paramList;
	}

	public NamedParamReplacer(String sql, Map<String, Object> params) {
	    this(sql, params, false);
	}
	public String preparedStatement() {
		return this.preparedStatement;
	}

	public List<Object> parameters() {
		return this.parameters;
	}
}
