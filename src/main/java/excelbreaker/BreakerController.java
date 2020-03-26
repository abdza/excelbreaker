package excelbreaker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.text.StringSubstitutor;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.thymeleaf.TemplateSpec;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring5.SpringTemplateEngine;

@Controller
public class BreakerController {
	
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
	
	@RequestMapping(value="/",method=RequestMethod.POST)
	public String upload(@RequestParam("file") MultipartFile file,Model model) {
		String curfolder = System.getProperty("user.dir");
		String filepath = curfolder + "/" + file.getOriginalFilename();
		List<String> headers = new ArrayList<String>();
		List<String> qheaders = new ArrayList<String>();
		try {
			System.out.println("Filepath:" + filepath);
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
		
		jdbctemplate.execute("create table filebreaker (id INT NOT NULL IDENTITY(1,1),CONSTRAINT PK_filebreaker PRIMARY KEY(id))");
		
		try {
			Workbook workbook = WorkbookFactory.create(curfile);
			Sheet sheet = workbook.getSheetAt(0);
			Iterator<Row> rows = sheet.rowIterator();
			Row drow;
			Boolean stoprow=false;
			Boolean firstrow=true;
			while(rows.hasNext() && !stoprow) {
				MapSqlParameterSource tparam = new MapSqlParameterSource();
				drow = rows.next();
				
				Iterator<Cell> cells = drow.cellIterator();					
				Cell cell;
				Integer hpos = 0;
				String dquery = null;
				if(!firstrow) {
					dquery = "insert into filebreaker (" + String.join(",", qheaders) + ") values (:" + String.join(",:" , qheaders) + ")";
				}
				while(cells.hasNext()) {
					cell = cells.next();
					System.out.println("hpos:" + hpos.toString());
					if(cell.getCellType()==CellType.NUMERIC) {
						Double curcontent = cell.getNumericCellValue();
						System.out.println("Content:" + curcontent.toString());
						if(!firstrow && !headers.get(hpos).equals("id")) {
							tparam.addValue(headers.get(hpos), curcontent.toString());
						}
					}
					else {
						String curcontent = cell.getStringCellValue();
						System.out.println("Content:" + curcontent);
						if(firstrow) {
							headers.add(curcontent);
							if(!curcontent.equals("id")) {
								qheaders.add(curcontent);
								String query = "alter table filebreaker add "+ curcontent.replaceAll(" ", "_") +" varchar(256) NULL";
								System.out.println("q:" + query);
								jdbctemplate.execute(query);	
							}				
						}
						else {
							if(!headers.get(hpos).equals("id")) {
								tparam.addValue(headers.get(hpos), curcontent.toString());
							}
						}
					}
					hpos+=1;
				}
				if(!firstrow) {
					System.out.println("tparam:" + tparam.toString());
					namedjdbctemplate.update(dquery, tparam);
				}
				firstrow = false;				
			}
			workbook.close();
			request.getSession().setAttribute("headers", headers);
			System.out.println("Headers:" + headers.toString());
		} catch (EncryptedDocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "redirect:/setup";
	}
	
	@RequestMapping(value="/setup",method=RequestMethod.GET)
	public String setup(Model model) {
		List<String> headers = (ArrayList<String>)request.getSession().getAttribute("headers");
		System.out.println("Here headers:" + headers.toString());
		model.addAttribute("headers",headers);
		return "setup.html";
	}
	
	@RequestMapping(value="/setup",method=RequestMethod.POST)
	public String breakit(Model model) {		
		List<String> headers = (ArrayList<String>)request.getSession().getAttribute("headers");
		headerfilter("gender","gender_${activefilter}.xlsx",headers);
		return "redirect:/";
	}
	
	public void headerfilter(String headerfilter, String outputfile, List<String> headers) {
		MapSqlParameterSource paramsource = new MapSqlParameterSource();
		String dquery = "select distinct " + headerfilter + " from filebreaker";
		SqlRowSet toret = namedjdbctemplate.queryForRowSet(dquery, paramsource);
		String curfolder = System.getProperty("user.dir");
		List<String> filtervalues = new ArrayList<String>();
		
		while(toret.next()) {
			filtervalues.add((String) toret.getObject(1));
		}
		System.out.println("Filter values:" + filtervalues.toString());
		
		for(String filter : filtervalues ) {
			String inquery = "select * from filebreaker where " + headerfilter + "=:" + headerfilter;
			MapSqlParameterSource psource = new MapSqlParameterSource();
			psource.addValue(headerfilter, filter);
			SqlRowSet inresult = namedjdbctemplate.queryForRowSet(inquery, psource);
			
			HashMap<String,Object> filevar = new HashMap<String,Object>();
			filevar.put("activefilter", filter.toLowerCase().replaceAll(" ","_"));
			
			StringSubstitutor sub = new StringSubstitutor(filevar);
			String outputloc = sub.replace(outputfile);
			
			outputloc = curfolder + "/" + outputloc;
			
			Workbook outexcel = new HSSFWorkbook();
			CreationHelper createHelper = outexcel.getCreationHelper();
			Sheet sheet = outexcel.createSheet("Sheet");
			
			Row headerRow = sheet.createRow(0);
			for(int i=0;i<headers.size();i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers.get(i));
			}
			
			int rowNum = 1;
			ArrayList<HashMap<String,Object>> rows = new ArrayList<HashMap<String,Object>>(); 
			while(inresult.next()) {
				Row row = sheet.createRow(rowNum);
				HashMap<String,Object> datarow = new HashMap<String,Object>();
				for(int i=0;i<headers.size();i++) {
					String header = headers.get(i);
					Object curval = inresult.getObject(header);
					row.createCell(i).setCellValue(curval.toString());
					datarow.put(headers.get(i),curval);
				}
				System.out.println("datarow:" + datarow.toString());				
				rows.add(datarow);
				rowNum+=1;
			}
			
			for(int i=0;i<headers.size();i++) {
				sheet.autoSizeColumn(i);
			}
			
			try {
				FileOutputStream fileOut = new FileOutputStream(outputloc);
				outexcel.write(fileOut);
				fileOut.close();
				outexcel.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

}
