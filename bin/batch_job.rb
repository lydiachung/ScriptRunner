require 'fileutils'
require 'yaml'
require 'java'
require 'bin/job_logger'

java_import 'java.util.concurrent.Executor'
java_import 'java.util.concurrent.Executors'
java_import 'java.util.concurrent.CountDownLatch'
java_import 'java.lang.Runnable'

class BatchJob
  include JobLogger
  
  def run(s_yaml_file)
  
    begin
    
      # logger
      @s_log_file = File.join(Dir.pwd, "log", "#{s_yaml_file}.log")
    
      h_config = YAML.load_file("#{s_yaml_file}.yml")
      h_settings = h_config['settings']
      h_settings["log_file"] = @s_log_file
      a_tasks = h_config['tasks']
   
      write_log("INFO", "=" * 80)
      write_log("INFO", "Script started")
      
      a_tasks.each do |x_task|
        
        case 
        when x_task.kind_of?(Array)
        
          a_sub_tasks = x_task
          o_signal = CountDownLatch.new(a_sub_tasks.length)
          o_executor = Executors.new_fixed_thread_pool(a_sub_tasks.length)
          
          a_sub_tasks.each do |h_task|
            h_options = h_settings.merge(h_task)
            if h_task.has_key?("prg")
              o_job = RoutineJob.new(h_options, o_signal)
            else
              o_job = CommandJob.new(h_options, o_signal)
            end
            o_executor.execute(o_job)
          end
          
          # initiates an orderly shutdown in which previously submitted tasks are executed, 
          # but no new tasks will be accepted.
          o_executor.shutdown()
          o_signal.await()
          
        when x_task.has_key?("prg")
          h_task = x_task
          h_options = h_settings.merge(h_task)
          o_job = RoutineJob.new(h_options)
          o_job.run()
          
        else
          h_task = x_task
          h_options = h_settings.merge(h_task)
          o_job = CommandJob.new(h_options)
          o_job.run()
        end
      end
      
      write_log("INFO", "Script finished")
      write_log("INFO", "=" * 80)
      
    rescue Exception => o_exc
      write_log("ERROR", "Batch job failed")
      write_log("ERROR", o_exc.message)
      write_log("ERROR", o_exc.inspect)
      write_log("ERROR", o_exc.backtrace)
    end
  end
end

class Job
  include Runnable
  include JobLogger
  
  def initialize(h_options, o_signal=nil)
    @h_options = h_options
    @s_log_file = h_options["log_file"]
    @o_signal = o_signal
  end
  
end

class RoutineJob < Job
  
  def run()
    
    s_prg_name = @h_options["prg"]
    s_exec_lbl = @h_options["lbl"]
    s_exec_lbl = s_prg_name if s_exec_lbl.nil? 
    s_argument = @h_options["arg"]
    s_work_dir = @h_options["dir"] # could be either settings.dir or tasks.dir
    s_exe_name = @h_options["exe"]
    s_user_name = @h_options["user"]
    
    Dir.chdir(s_work_dir) do 
      s_command = %Q{#{s_exe_name} #{s_user_name} #{s_prg_name} "#{s_argument}"}
      write_log("INFO", "#{s_exec_lbl} started")
      write_log("DEBUG", s_command)
      write_log("INFO", %x{#{s_command}})
      write_log("INFO", "#{s_exec_lbl} finished")
    end
    
    @o_signal.count_down() unless @o_signal.nil?
  end
end

class CommandJob < Job

  def run()
    
    s_exec_lbl = @h_options["lbl"]
    s_exec_cmd = @h_options["cmd"]
    s_work_dir = @h_options["dir"]
    
    Dir.chdir(s_work_dir) do
      write_log("INFO", "#{s_exec_lbl} started")
      write_log("DEBUG", s_exec_cmd)
      write_log("INFO", %x{#{s_exec_cmd} 2>&1})
      write_log("INFO", "#{s_exec_lbl} finished")
    end
    
    @o_signal.count_down() unless @o_signal.nil?
  end

end


BatchJob.new().run(ARGV[0])