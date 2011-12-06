require 'timeout'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/core_ext/kernel'
require 'logger'

module Delayed
  class Worker
    DEFAULT_BATCH_SIZE = 500

    cattr_accessor :priority, :batch_size, :max_attempts, :max_run_time, :default_priority, :sleep_delay, :logger, :delay_jobs
    self.sleep_delay = 2.minutes
    self.max_attempts = 5
    self.max_run_time = 4.hours
    self.default_priority = 0
    self.delay_jobs = true

    # By default failed jobs are destroyed after too many attempts. If you want to keep them around
    # (perhaps to inspect the reason for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true

    self.logger = if defined?(Rails)
      Rails.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    # name_prefix is ignored if name is set directly
    attr_accessor :name_prefix

    cattr_reader :backend

    def self.backend=(backend)
      if backend.is_a? Symbol
        require "delayed/serialization/#{backend}"
        require "delayed/backend/#{backend}"
        backend = "Delayed::Backend::#{backend.to_s.classify}::Job".constantize
      end
      @@backend = backend
      silence_warnings { ::Delayed.const_set(:Job, backend) }
    end

    def self.guess_backend
      self.backend ||= :active_record if defined?(ActiveRecord)
    end

    def initialize(options={})
      @quiet = !options.has_key?(:quiet) || options[:quiet]
      self.class.batch_size = options.has_key?(:batch_size) ? options[:batch_size] : DEFAULT_BATCH_SIZE
      self.class.priority = options[:priority] if options.has_key?(:priority)
      self.class.sleep_delay = options[:sleep_delay] if options.has_key?(:sleep_delay)
    end

    # Every worker has a unique name which by default is the pid of the process. There are some
    # advantages to overriding this with something which survives worker retarts:  Workers can#
    # safely resume working on tasks which are locked by themselves. The worker will assume that
    # it crashed before.
    def name
      return @name unless @name.nil?
      "#{@name_prefix}host:#{Socket.gethostname} pid:#{Process.pid}" rescue "#{@name_prefix}pid:#{Process.pid}"
    end

    # Sets the name of the worker.
    # Setting the name to nil will reset the default worker name
    def name=(val)
      @name = val
    end

    def start
      say "Starting job worker"

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = work_off
        end

        count = result.sum

        break if $exit

        if count.zero?
          say "Nothing more to do."
          sleep(self.class.sleep_delay)
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end

        break if $exit
      end
    end

    def remove_successful_jobs(jobs)
      return if jobs.empty?
      
      job_ids = jobs.collect {|j| j.id}.join(",")
      ActiveRecord::Base.connection.execute("DELETE FROM delayed_jobs WHERE id IN (#{job_ids})")
    end

    def mark_failed_jobs(jobs)
      return if jobs.empty?
      queries = []
      queries << "DROP TABLE IF EXISTS delayed_job_updates"
      queries << "CREATE TEMPORARY TABLE delayed_job_updates(id int(11), failed_at datetime, attempts int(11), last_error text, run_at datetime, PRIMARY KEY(id))"
      inserts = jobs.collect do |j| 
        last_error = j.last_error.gsub(/\\/, '\&\&').gsub(/'/, "''")
        failed_at = j.failed_at ? "'#{j.failed_at.to_s(:db)}'" : "null"
        "(#{j.id}, #{failed_at}, #{j.attempts}, '#{last_error}', '#{j.run_at}')" 
      end.join(",")
    
      queries << "INSERT INTO delayed_job_updates(id, failed_at, attempts, last_error, run_at) VALUES #{inserts}"
      queries << "UPDATE delayed_jobs, delayed_job_updates SET delayed_jobs.failed_at = delayed_job_updates.failed_at, delayed_jobs.attempts = delayed_job_updates.attempts, delayed_jobs.last_error = delayed_job_updates.last_error, delayed_jobs.run_at = delayed_job_updates.run_at WHERE delayed_jobs.id = delayed_job_updates.id"
      queries << "DROP TABLE IF EXISTS delayed_job_updates"
      queries.each do |query|
        ActiveRecord::Base.connection.execute(query)
      end
    end

    def work_off
      jobs = Delayed::Job.next_available_batch(self.class.priority, self.class.batch_size)
      
      jobs_by_status = {:success => [], :failure => []}
      say "Working..."
      jobs.each do |job|
        if run(job)
          jobs_by_status[:success] << job
        else
          jobs_by_status[:failure] << job
        end
      end
      say "Removing successful jobs and updating failed ones..."
      remove_successful_jobs(jobs_by_status[:success])
      
      mark_failed_jobs(jobs_by_status[:failure])
      
      say "Done."
      
      return [jobs_by_status[:success].length, jobs_by_status[:failure].length]
    end

    def run(job)
      runtime =  Benchmark.realtime do
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
      end
      say "#{job.name} completed after %.4f" % runtime
      return true  # did work
    rescue Exception => error
      say "#{job.name} failed."
      puts error.message
      puts error.backtrace.join("\n")

      job.last_error = "{#{error.message}\n#{error.backtrace.join('\n')}"
      job.attempts += 1
      if job.attempts > self.class.max_attempts
        job.failed_at = Time.now
      else
        job.run_at = Time.now + (5**job.attempts).minutes
      end

      return false
    end

    def say(text, level = Logger::ERROR)
      text = "[Worker(#{name})] #{text}"
      puts text unless @quiet
      logger.add level, "#{Time.now.strftime('%FT%T%z')}: #{text}" if logger
    end

    def max_attempts(job)
      job.max_attempts || self.class.max_attempts
    end
    
  end

end
