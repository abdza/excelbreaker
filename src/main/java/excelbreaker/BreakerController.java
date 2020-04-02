package excelbreaker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.text.StringSubstitutor;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

@Controller
public class BreakerController {
	
    enum xssfDataType {
        BOOL, ERROR, FORMULA, INLINESTR, SSTINDEX, NUMBER,
    }

	
	@Autowired
	private JdbcTemplate jdbctemplate;
	
	@Autowired
	private NamedParameterJdbcTemplate namedjdbctemplate;
	
	@Autowired
	private HttpServletRequest request;
	
	@RequestMapping(value="/",method=RequestMethod.GET)
	public String main(Model model) {
		return "main.html";
	}
	
	public String lastvalue=null;
	
	
	public void processOneSheet(String filename) throws Exception {
        OPCPackage pkg = OPCPackage.open(filename);
        XSSFReader r = new XSSFReader( pkg );
        ReadOnlySharedStringsTable sst = new ReadOnlySharedStringsTable(pkg);
        StylesTable styles = r.getStylesTable();
        XMLReader parser = fetchSheetParser(sst,styles);
        InputStream sheet = r.getSheetsData().next();
        InputSource sheetSource = new InputSource(sheet);
        if(sheetSource!=null) {
        	parser.parse(sheetSource);
        }
        sheet.close();
        pkg.close();
    }
	
	public XMLReader fetchSheetParser(ReadOnlySharedStringsTable sst,StylesTable styles) throws SAXException, ParserConfigurationException {
        XMLReader parser = XMLHelper.newXMLReader();
        ContentHandler handler = new SheetHandler(sst,styles,jdbctemplate,namedjdbctemplate,request);
        parser.setContentHandler(handler);
        return parser;
    }
    /**
     * See org.xml.sax.helpers.DefaultHandler javadocs
     */
    private static class SheetHandler extends DefaultHandler {
        private ReadOnlySharedStringsTable sst;
        private String lastContents;
        private boolean nextIsString;
        private String dquery;
        private boolean firstRow = true;
        private int hpos;
        private HttpServletRequest inrequest;
        // Used to format numeric cell values.
        private short formatIndex;
        private String formatString;
        private StylesTable stylesTable;
        private xssfDataType nextDataType;


        private final DataFormatter formatter;

        
        private JdbcTemplate injdbctemplate;
        
        private NamedParameterJdbcTemplate innamedjdbctemplate;
        
        List<String> headers = new ArrayList<String>();
        List<String> qheaders = new ArrayList<String>();
        
        private SheetHandler(ReadOnlySharedStringsTable sst,StylesTable styles,JdbcTemplate injdbctemplate,NamedParameterJdbcTemplate innamedjdbctemplate,HttpServletRequest inrequest) {
            this.sst = sst;
            this.stylesTable = styles;
            this.injdbctemplate = injdbctemplate;
            this.innamedjdbctemplate = innamedjdbctemplate;
            this.inrequest = inrequest;
            this.nextDataType = xssfDataType.NUMBER;
            this.formatter = new DataFormatter();
        }
        MapSqlParameterSource tparam;
        public void startElement(String uri, String localName, String name,
                                 Attributes attributes) throws SAXException {
            // c => cell
            if(name.equals("c")) {
                this.nextDataType = xssfDataType.NUMBER;
                this.formatIndex = -1;
                this.formatString = null;
                String cellType = attributes.getValue("t");
                if(cellType != null && cellType.equals("s")) {
                    nextIsString = true;
                } else {
                    nextIsString = false;
                }
                String cellStyleStr = attributes.getValue("s");
                if ("b".equals(cellType))
                    nextDataType = xssfDataType.BOOL;
                else if ("e".equals(cellType))
                    nextDataType = xssfDataType.ERROR;
                else if ("inlineStr".equals(cellType))
                    nextDataType = xssfDataType.INLINESTR;
                else if ("s".equals(cellType))
                    nextDataType = xssfDataType.SSTINDEX;
                else if ("str".equals(cellType))
                    nextDataType = xssfDataType.FORMULA;
                else if (cellStyleStr != null) {
                    // It's a number, but almost certainly one
                    // with a special style or format
                    int styleIndex = Integer.parseInt(cellStyleStr);
                    XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
                    this.formatIndex = style.getDataFormat();
                    this.formatString = style.getDataFormatString();
                    if (this.formatString == null)
                        this.formatString = BuiltinFormats
                                .getBuiltinFormat(this.formatIndex);
                }
            }
            if(name.equals("row")) {
            	tparam = new MapSqlParameterSource();
            	hpos = 0;
            	if(!firstRow) {
            		dquery = "insert into filebreaker (" + String.join(",", qheaders) + ") values (:" + String.join(",:" , qheaders) + ")";
            	}
            }
            // Clear contents cache
            lastContents = "";
        }
        public void endElement(String uri, String localName, String name)
                throws SAXException {
            // Process the last contents as required.
            // Do now, as characters() may be called more than once
        	if(nextIsString) {
                int idx = Integer.parseInt(lastContents);
                lastContents = sst.getItemAt(idx).getString();
                nextIsString = false;
            }
            // v => contents of a cell
            // Output after we've seen the string contents
            if(name.equals("v")) {
                if(firstRow) {
                	
                	headers.add(lastContents.replaceAll(" ", "_"));
                	if(!lastContents.equals("id") && lastContents!=null) {
                		qheaders.add(lastContents.replaceAll(" ", "_"));
                		dquery = "alter table filebreaker add "+ lastContents.replaceAll(" ", "_") +" varchar(256) NULL";
                		injdbctemplate.execute(dquery);
                	}
                	
                }
                else {
                    String thisStr = lastContents;

                    switch (nextDataType) {

                    case NUMBER:
                        String n = lastContents.toString();
                        if (this.formatString != null)
                            thisStr = formatter.formatRawCellContents(Double
                                    .parseDouble(n), this.formatIndex,
                                    this.formatString);
                        else
                            thisStr = n;
                        break;
                    }

                	
                	tparam.addValue(headers.get(hpos), thisStr);
                }
                hpos+=1;
            }
            if(name.equals("row")) {
            	if(!firstRow) {
            		innamedjdbctemplate.update(dquery, tparam);
            		
            	}        
            	else {
            		firstRow=false;
            		inrequest.getSession().setAttribute("headers", headers);
            	}
            }
        }
        public void characters(char[] ch, int start, int length) {
            lastContents += new String(ch, start, length);
        }
    }
	
	
	
	@RequestMapping(value="/",method=RequestMethod.POST)
	public String upload(@RequestParam("file") MultipartFile file,Model model) {
		String curfolder = System.getProperty("user.dir");
		String filepath = curfolder + "/" + file.getOriginalFilename();
		List<String> headers = new ArrayList<String>();
		List<String> qheaders = new ArrayList<String>();
		try {
			file.transferTo(new File(filepath));
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		request.getSession().setAttribute("filepath", filepath);
		File curfile = new File(filepath);
		jdbctemplate.execute("drop table filebreaker if exists");
		jdbctemplate.execute("create table filebreaker (id INT NOT NULL IDENTITY(1,1),CONSTRAINT PK_filebreaker PRIMARY KEY(id))");
		
		try {
			processOneSheet(filepath);
			curfile.delete();
			System.gc();
		} catch (EncryptedDocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "redirect:/setup";
	}
	
	@RequestMapping(value="/setup",method=RequestMethod.GET)
	public String setup(Model model) {
		List<String> headers = (ArrayList<String>)request.getSession().getAttribute("headers");
		model.addAttribute("headers",headers);
		return "setup.html";
	}
	
	@RequestMapping(value="/setup",method=RequestMethod.POST)
	public String breakit(Model model) {		
		List<String> headers = (ArrayList<String>)request.getSession().getAttribute("headers");
		Map<String, String[]> postdata = request.getParameterMap();
		for(String header:headers) {
			if(postdata.get("head_" + header)!=null && postdata.get("head_" + header)[0].length()>3) {
				Boolean hidefilter = false;
				if(postdata.get("hide_" + header)!=null) {
					hidefilter = true;
				}
				String sheetname = "Sheet 1";
				if(postdata.get("sheet_" + header)!=null && postdata.get("sheet_" + header)[0].length()>3) {
					sheetname = postdata.get("sheet_" + header)[0];
				}
				headerfilter(header,postdata.get("head_" + header)[0],hidefilter,sheetname,headers);
			}
		}
		return "redirect:/";
	}
	
	public void headerfilter(String headerfilter, String outputfile, Boolean hidefilter, String sheetname, List<String> headers) {
		List<String> qheaders = new ArrayList<String>();
		for(String header:headers) {
			if(!header.equals(headerfilter) || !hidefilter) {
				qheaders.add(header);
			}
		}
		MapSqlParameterSource paramsource = new MapSqlParameterSource();
		String dquery = "select distinct " + headerfilter + " from filebreaker";
		SqlRowSet toret = namedjdbctemplate.queryForRowSet(dquery, paramsource);
		String curfolder = System.getProperty("user.dir");
		List<String> filtervalues = new ArrayList<String>();
		
		while(toret.next()) {
			filtervalues.add((String) toret.getObject(1));
		}
		
		for(String filter : filtervalues ) {
			String inquery = "select * from filebreaker where " + headerfilter + "=:" + headerfilter;
			MapSqlParameterSource psource = new MapSqlParameterSource();
			psource.addValue(headerfilter, filter);
			SqlRowSet inresult = namedjdbctemplate.queryForRowSet(inquery, psource);
			
			HashMap<String,Object> filevar = new HashMap<String,Object>();
			filevar.put("activefilter", filter.toLowerCase().replaceAll(" ","_"));
			
			StringSubstitutor sub = new StringSubstitutor(filevar);
			String outputloc = sub.replace(outputfile);
			
			String finalsheetname = sub.replace(sheetname);
			
			outputloc = curfolder + "/output/" + outputloc;
			
			String[] pathparts = outputloc.split("/");
			String justpath = String.join("/", Arrays.copyOfRange(pathparts, 0, pathparts.length-1));
			File fpath = new File(justpath);			
			if(!fpath.exists()) {
				fpath.mkdirs();
			}
			
			SXSSFWorkbook wb = new SXSSFWorkbook(100); // keep 100 rows in memory, exceeding rows will be flushed to disk
			Sheet sheet = wb.createSheet();
		       
			Row headerRow = sheet.createRow(0);
			for(int i=0;i<qheaders.size();i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(qheaders.get(i).replaceAll("_", " "));
			}
			
			int rowNum = 1;
			ArrayList<HashMap<String,Object>> rows = new ArrayList<HashMap<String,Object>>(); 
			while(inresult.next()) {
				Row row = sheet.createRow(rowNum);
				HashMap<String,Object> datarow = new HashMap<String,Object>();
				for(int i=0;i<qheaders.size();i++) {
					String header = qheaders.get(i);
					Object curval = inresult.getObject(header);
					row.createCell(i).setCellValue(curval.toString());
				}
				rowNum+=1;
			}
			
			/* for(int i=0;i<qheaders.size();i++) {
				sheet.autoSizeColumn(i);
			}*/
			
			try {
				FileOutputStream fileOut = new FileOutputStream(outputloc);
				wb.write(fileOut);
				fileOut.close();
				
				wb.dispose();
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.gc();
		}
	}

}
