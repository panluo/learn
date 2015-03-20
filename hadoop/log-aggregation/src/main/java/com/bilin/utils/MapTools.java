package com.bilin.utils;

import com.bilin.ip.IpToGeo;
import com.bilin.main.Config;

import org.apache.hadoop.io.Text;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class MapTools {

    public StringBuffer strbuff = new StringBuffer();

    public String[] dimension;

    public static final String COMMA = ",";

    public static final String POINT = ".";

    public static final String UNKNOWN = "unknown";

    public static final String DELIM = "|";

    public ArrayList<String> splits = new ArrayList<String>();

    public StringBuffer geo = new StringBuffer();

    public Text buildKey(String str, ArrayList<String> splits) {
        dimension = Config.dimensionsMap.get(str).split(COMMA);
        strbuff.setLength(0);
        String dimensionNM = str.substring(str.indexOf(POINT) + 1);
        strbuff.append(dimensionNM);
        int i = 0, length = dimension.length;
        for (; i < length - 2; i++) {
            strbuff.append(DELIM);
            if (!"".equals(splits.get(Config.logFormat.get(dimension[i]))) && !" ".equals(splits.get(Config.logFormat
                    .get(dimension[i]))) && !"nil".equals(splits.get(Config.logFormat.get(dimension[i]))))
                strbuff.append(splits.get(Config.logFormat.get(dimension[i])));
            else
                strbuff.append(UNKNOWN);
        }
        strbuff.append(DELIM);
        if (dimensionNM.equals("geo")) {
            if ("bidswitch".equals(splits.get(Config.logFormat.get("ad_exchange"))))
                strbuff.append(extractGeo(splits.get(Config.logFormat.get(dimension[i]))));
            else {
                String ipString = splits.get(Config.logFormat.get("user_ip"));
                String ipCode = IpToGeo.lookup(ipString);
                if (ipCode != null) {
                    strbuff.append(ipCode);
                } else {
                    strbuff.append(UNKNOWN);
                }
            }
        } else if (dimensionNM.equals("domain")) {
            String url = splits.get(Config.logFormat.get("url"));
//            strbuff.append(extractDomain(url));
            strbuff.append(extractDomainFromUrl(url));
        } else if (!"".equals(splits.get(Config.logFormat.get(dimension[i]))) && !" ".equals(splits.get(Config
                .logFormat.get(dimension[i]))) && !"nil".equals(splits.get(Config.logFormat.get(dimension[i])))) {
            strbuff.append(splits.get(Config.logFormat.get(dimension[i])));
        } else
            strbuff.append(UNKNOWN);
        strbuff.append(DELIM);
        strbuff.append(Long.parseLong(splits.get(Config.logFormat.get(dimension[length - 1]))) / 3600);
        return new Text(strbuff.toString());
    }

    public String extractGeo(String geostr) {
        if (isNull(geostr)) {
            String geo = "";
            String[] geo_sp = geostr.split("\\|");
            if (0 == geo_sp.length)
                return UNKNOWN;
            else {
                for (int j = 0; j < geo_sp.length && j < 3; j++) {
                    geo += geo_sp[j];
                    geo += "-";
                    if ("".equals(geo_sp[j]))
                        break;
                }
                if ("".equals(geo.substring(0, geo.length() - 1)))
                    return UNKNOWN;
                return geo.substring(0, geo.length() - 1);
            }
        } else
            return UNKNOWN;
    }

    public boolean isNull(String str) {
        return !"".equals(str) && !" ".equals(str) && !"nil".equals(str);
    }

    public String extractDomain(String url) {
        if (url.equals(" ") || url.equals(""))
            return UNKNOWN;
        StringTokenizer st = new StringTokenizer(url, "/");
        st.nextToken();
        String tmp;
        if (st.hasMoreTokens()) {
            tmp = st.nextToken();
            int first = tmp.indexOf(".");
            if (tmp.contains("www.")) {
                return tmp.substring(first + 1);

            } else {
                return tmp;
            }
        } else return UNKNOWN;
    }

    public String extractDomainFromUrl(String url) {
        URL myurl;
        try {
            myurl = new URL(url);
            String host = myurl.getHost();
            if (host.startsWith("www.")) {
                host = host.substring(host.indexOf("www.") + 4);
            }
            if ("".equals(host))
                return UNKNOWN;
            else
                return host;
        } catch (MalformedURLException e) {
            return UNKNOWN;
        }
    }

    public void buildGEO(String geo_info) {
        StringTokenizer st = new StringTokenizer(geo_info, "_");
        int times = 0;
        geo.setLength(0);
        while (st.hasMoreElements() && 2 > times) {
            geo.append(st.nextToken()).append('-');
            times++;
        }
    }

    public ArrayList<String> split(String str, String delim) {
        splits.clear();
        int posBeg = 0, length = str.length(), posEnd = str.indexOf(delim, posBeg);
        while (length != posEnd && -1 != posEnd) {
            splits.add(str.substring(posBeg, posEnd));
            posBeg = posEnd + delim.length();
            posEnd = str.indexOf(delim, posBeg);
        }
        splits.add(str.substring(posBeg, length));
        return splits;
    }
}
