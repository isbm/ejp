/**
 * Copyright (C) 2006 - present David Bulmore  
 * All Rights Reserved.
 *
 * This file is part of Easy Java Persistence.
 *
 * EJP is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the accompanying license 
 * for more details.
 *
 * You should have received a copy of the license along with EJP; if not, 
 * go to http://www.EasierJava.com and download the latest version.
 */

package ejp.utilities;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXParseException;

public abstract class XMLParser
  {
    private static Logger logger = LoggerFactory.getLogger(XMLParser.class);
    protected XMLParser() {}
    
    protected XMLParser(String filename, boolean validate) throws XMLParserException
      {
        loadData(filename, validate);
      }
  
    protected XMLParser(URL fileUrl, boolean validate) throws XMLParserException
      {
        loadData(fileUrl, validate);
      }
  
    protected void loadData(String filename, boolean validate) throws XMLParserException
      {
        try
          {
            loadData(new FileInputStream(filename), validate);
          }
        catch (Exception e)
          {
            throw new XMLParserException(e);
          }
      }
    
    protected void loadData(URL fileUrl, boolean validate) throws XMLParserException
      {
        try
          {
            loadData(fileUrl.openStream(), validate);
          }
        catch (Exception e)
          {
            throw new XMLParserException(e);
          }
      }
    
    protected void loadData(InputStream in, boolean validate) throws XMLParserException
      {
        try
          {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();

            f.setValidating(validate);
        
            DocumentBuilder db = f.newDocumentBuilder();

            db.setErrorHandler(new XMLErrorHandler());

            Document d = db.parse(in);

            in.close();

            Element e = d.getDocumentElement();

            processXML(e);
          }
        catch (Exception ex)
          {
            throw new XMLParserException(ex);
          }
      }

    static class XMLErrorHandler implements ErrorHandler
      {
        public void error(SAXParseException exception)
          {
            logger.error("Error: at line " + exception.getLineNumber() + ", column " + exception.getColumnNumber(), exception);
            exception.printStackTrace(System.err);
          }

        public void fatalError(SAXParseException exception)
          {
            logger.error("Error: at line " + exception.getLineNumber() + ", column " + exception.getColumnNumber(),exception);
            exception.printStackTrace(System.err);
          }

        public void warning(SAXParseException exception) 
          {
            logger.error("Error: at line " + exception.getLineNumber() + ", column " + exception.getColumnNumber(),exception);
            exception.printStackTrace(System.err);
          }
      }
    
    protected abstract void processXML(Element e) throws Exception;
  }

