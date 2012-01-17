namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => :environment do
    Delayed::Job.delete_all
  end

  desc "Start a delayed_job worker."
  task :work => :environment do
    Delayed::Worker.new(:priority => ENV['PRIORITY'], :batch_size => ENV['BATCH_SIZE'].to_i, :single_batch => (ENV['SINGLE_BATCH']=='1'), :sleep_delay => (ENV['SLEEP_DELAY']||300).to_i, :verbose => (ENV['VERBOSE']=='true')).start
  end
end
