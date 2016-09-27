package com.juanpi.bi.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class DwPageValue
{
  private static DwPageValue pagevalueChecker = null;
  public static Log log = LogFactory.getLog(DwPageValue.class);
  public List<String> regex_pattern;
  public static List<Page_Pattern> patterns = new ArrayList();
  public static Map<String, Page_Pattern> patternsMap = new HashMap();

  public static DwPageValue getInstance(String paramString)
  {
    if (pagevalueChecker == null)
    {

      pagevalueChecker = new DwPageValue(paramString);
    }
    return pagevalueChecker;
  }

  private DwPageValue(String paramString)
  {
    loadPcPageValue(paramString);
    loadPcPageValueMap(paramString);
  }

  public boolean match(String paramString)
  {
    Iterator localIterator = this.regex_pattern.iterator();
    while (localIterator.hasNext())
    {
      String str1 = (String)localIterator.next();
      if (str1.contains("%"))
      {
        String[] arrayOfString1 = str1.split("%");
        int i = 1;
        for (String str2 : arrayOfString1)
        {
          if ((str2.trim().length() <= 0) || (paramString.contains(str2.trim())))
            continue;
          i = 0;
          break;
        }
        if (i != 0)
        {
          if ((!str1.startsWith("%")) && (!paramString.startsWith(arrayOfString1[0])))
            i = 0;
          if ((!str1.endsWith("%")) && (!paramString.endsWith(arrayOfString1[(arrayOfString1.length - 1)])))
            i = 0;
        }
        if (i != 0)
          return true;
      }
      else if (paramString.matches(str1))
      {
        return true;
      }
    }
    return false;
  }

  private static List<Page_Pattern> loadPcPageValue(String paramString)
  {
    try
    {
      log.info("Start to loading new page pattern ...." + paramString);
      InputStream localInputStream = DwPageValue.class.getClassLoader().getResourceAsStream(paramString);
//      System.out.println("localInputStream=====" + localInputStream);
      BufferedReader localBufferedReader = new BufferedReader(new InputStreamReader(localInputStream, "UTF-8"));
      String str1 = null;
      while ((str1 = localBufferedReader.readLine()) != null)
      {
        String[] arrayOfString = str1.split(",");
        if (arrayOfString.length < 3)
          break;
        Page_Pattern localPage_Pattern = new Page_Pattern();
        localPage_Pattern.page_value = arrayOfString[0];
        localPage_Pattern.page_type_id = arrayOfString[1];
        localPage_Pattern.regex_pattern = new ArrayList();
        for (int i = 2; i < arrayOfString.length; i++)
        {
          String str2 = arrayOfString[i].trim();
          if ((str2.length() <= 0) || ("null".equals(str2)))
            continue;
          localPage_Pattern.regex_pattern.add(str2.toLowerCase());
        }
        patterns.add(localPage_Pattern);
      }
      log.info("The new version page pattern has been load successfully.paramString = " + paramString);
    }
    catch (Exception localException)
    {
      log.info("Failed to load the new page pattern: " + paramString + ", Exception", localException);
      return null;
    }
    return patterns;
  }

  private static Map<String, Page_Pattern> loadPcPageValueMap(String paramString)
  {
    try
    {
      log.info("Start to loading new page pattern ...." + paramString);
      InputStream localInputStream = DwPageValue.class.getClassLoader().getResourceAsStream(paramString);
      BufferedReader localBufferedReader = new BufferedReader(new InputStreamReader(localInputStream, "UTF-8"));
      String str1 = null;
      while ((str1 = localBufferedReader.readLine()) != null)
      {
        String[] arrayOfString = str1.split(",");
        if (arrayOfString.length < 3)
          break;
        Page_Pattern localPage_Pattern = new Page_Pattern();
        localPage_Pattern.page_value = arrayOfString[0];
        localPage_Pattern.page_type_id = arrayOfString[1];
        localPage_Pattern.regex_pattern = new ArrayList();
        for (int i = 2; i < arrayOfString.length; i++)
        {
          String str2 = arrayOfString[i].trim();
          if ((str2.length() <= 0) || ("null".equals(str2)))
            continue;
          localPage_Pattern.regex_pattern.add(str2.toLowerCase());
        }
        patternsMap.put(arrayOfString[0], localPage_Pattern);
      }
      log.info("The new version page pattern has been load successfully.");
    }
    catch (Exception localException)
    {
      log.info("Failed to load the new page pattern: ", localException);
      return null;
    }
    return patternsMap;
  }

  public static class Page_Pattern
  {
    public String page_type_id;
    public String page_value;
    public List<String> regex_pattern;

    public boolean match(String paramString)
    {
      Iterator localIterator = this.regex_pattern.iterator();
      while (localIterator.hasNext())
      {
        String str1 = (String)localIterator.next();
        if (str1.contains("%"))
        {
          String[] arrayOfString1 = str1.split("%");
          int i = 1;
          for (String str2 : arrayOfString1)
          {
            if ((str2.trim().length() <= 0) || (paramString.contains(str2.trim())))
              continue;
            i = 0;
            break;
          }
          if (i != 0)
          {
            if ((!str1.startsWith("%")) && (!paramString.startsWith(arrayOfString1[0])))
              i = 0;
            if ((!str1.endsWith("%")) && (!paramString.endsWith(arrayOfString1[(arrayOfString1.length - 1)])))
              i = 0;
          }
          if (i != 0)
            return true;
        }
        else if (paramString.matches(str1))
        {
          return true;
        }
      }
      return false;
    }
  }

  public static void main(String[] args) {
    getInstance("PcPageValue.properties");
  }
}