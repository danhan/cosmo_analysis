package cos.dataset.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;

import cos.dataset.parser.CosmoConstant;


/*
 * based on the files to get the statistics 
 */
public class CosmoStatistics {
	
	private String input = null;
	private String statFile = null;
	private BufferedWriter bw = null;
	
	public CosmoStatistics(String input,String statFile){
		this.input = input;
		this.statFile = statFile;
		try{
			bw = new BufferedWriter(new FileWriter(this.statFile,true)); 
			bw.write("snapshot,type,num_of_particle,diff_max_min,min,max,num_of_attr,attr_list\n");
			bw.flush();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/*
	 * return total number of particles for each type, each snapshot, min particle index and max particle index
	 * format : snapshot,particletype,min_particle_index, max_particle_index
	 */
	
	public void doStatistics(boolean header){
					
		try{
			File dir = new File(this.input);
			if(dir.isDirectory()){				
				for(String filename: dir.list()){	
					if(!filename.endsWith("out"))
						continue;
					this.doStatistic4File(dir.getAbsolutePath()+"/"+filename, header);
				}				
			}else{
				this.doStatistic4File(this.input, header);
			}		
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try{
				this.bw.close();	
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}		
	}
	
	private void doStatistic4File(String filename,boolean header){
		System.out.println("start doing statistics on "+filename);
		
		BufferedReader br = null;
		try{
			String intermedia = filename.substring(filename.lastIndexOf('/'),filename.length()-1);
			long snapshot = Long.parseLong(intermedia.split(CosmoConstant.FILE_NAME_DELIMITER)[2]);	
			int type = 0;
			int attr_count = 0;
			String attr_list = null;
			if (filename.contains("gas")) {				
				type = 0;
				attr_count = CosmoConstant.ENUM_GAS.count();
				attr_list = CosmoConstant.ENUM_GAS.toSerialize();
			} else if (filename.contains("dark")) {
				type = 1;
				attr_count = CosmoConstant.ENUM_DARK_MATTER.count();
				attr_list = CosmoConstant.ENUM_DARK_MATTER.toSerialize();
			} else if (filename.contains("star")) {
				type = 2;
				attr_count = CosmoConstant.ENUM_STAR.count();
				attr_list = CosmoConstant.ENUM_STAR.toSerialize();
			}			
						
			br = new BufferedReader(new FileReader(filename));
			if(header) // there is a head which should be passed
				br.readLine();
			String line = br.readLine();
			long min = Long.MAX_VALUE;
			long max = Long.MIN_VALUE;
			int count = 0;
			while(line != null){
				line = line.trim();
				if(line.length()==0){
					continue;
				}
				String[] metrics = line.split(CosmoConstant.METRICS_DELIMETER);
				long pid = Long.parseLong(metrics[CosmoConstant.INDEX_PID]);
				count++;
				if(pid > max){
					max = pid;
				}
				if(pid < min){
					min = pid;
				}	
				
				line = br.readLine();
			}			
			br.close();
			
			DecimalFormat df = new DecimalFormat( "#");
			
			this.bw.write(snapshot+","+type+","+count+","+df.format((max-min)+1)+","+df.format(min)+","+df.format(max)+","+
					attr_count+","+attr_list+"\n");
			this.bw.flush();
			
			System.out.println("end doing statistics on "+filename);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String input = args[0];
		String statFile = args[1];
		CosmoStatistics stat = new CosmoStatistics(input,statFile);
		stat.doStatistics(true);

	}

}
