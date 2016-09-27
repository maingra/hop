=begin 
Net::SSH.start("host", "user") do |ssh|
  ssh.exec! "cp /some/file /another/location"
  hostname = ssh.exec!("hostname")

  ssh.open_channel do |ch|
    ch.exec "sudo -p 'sudo password: ' ls" do |ch, success|
      abort "could not execute sudo ls" unless success

      ch.on_data do |ch, data|
        print data
        if data =~ /sudo password: /
          ch.send_data("password\n")
        end
      end
    end
  end

  ssh.loop
end
=end
class Hadoop_FS
  # @@header = 'hdfs dfs -'
  @@header = 'hadoop fs -'
  def initialize
    @defualt_mode='u=rwx,g=rx,o=r'
    @default_owner='root'
  end
  attr_accessor :default_mode, :defualt_owner
  def appendToFile(localsrc,dst) #Appends the contents of all the given local files to the given dst file. The dst  file will be created if it does not exist. If <localSrc> is -, then the input is  read from stdin.
    cmd = "appendToFile #{localsrc} #{dst}"
    return @@header + cmd
  end	
  def cat(src,crc=false) #  Fetch all files that match the file pattern <src> and display their content on stdout.
    cmd = "cat -ignoreCrc #{src}" unless crc
    cmd = "cat  #{src}" if crc
    return @@header + cmd
  end
  def checksum(src) #Dump checksum information for files that match the file pattern <src> to stdout.
    cmd = "checksum  #{src}"
    return @@header + cmd
  end
  def chgrp(path,rec=false)
    rec ? cmd = "chgrp -R #{path}" : cmd = "chgrp #{path}"
    return @@header + cmd
  end
  def chmod(path,mode='u=rwx,g=rx,o=r',rec=false)
    mode = @defualt_mode
    rec ? cmd = "chmod -R #{mode} #{path}" : cmd = "chmod #{mode} #{path}"
    return @@header + cmd
  end
  def chown(path,owner='root',rec=false)
    owner = @default_owner
    rec ? cmd = "chown -R #{owner} #{path}" : cmd = "chown #{owner} #{path}"
    return @@header + cmd
  end
  def copyFromLocal(localsrc,dst,force=false,pres=false)
    if force then
      pres ? cmd = "put -f -p #{localsrc} #{dst}" : cmd = "put -f #{localsrc} #{dst}"
    else
      pres ? cmd = "put -p #{localsrc} #{dst}" : cmd = "put #{localsrc} #{dst}"
    end
    return @@header + cmd
  end
  def copyToLocal(localsrc,dst,pres=false,crc=false)
    if crc then
      pres ? cmd = "get -p -crc #{localsrc} #{dst}" : cmd = "get -crc #{localsrc} #{dst}"
    else
      pres ? cmd = "get -p -ignoreCrc #{localsrc} #{dst}" : cmd = "get -ignoreCrc #{localsrc} #{dst}"
    end
    return @@header + cmd
  end
  def count(path)
    cmd = "count -q -h -v #{path}"
    return @@header + cmd
  end
  def cp(src,dst,force=false,pres=false)
    if force then
      pres ? cmd = "cp -f -p #{src} #{dst}" : cmd = "cp -f #{src} #{dst}"
    else
      pres ? cmd = "cp -p #{src} #{dst}" : cmd = "cp #{src} #{dst}"
    end
    return @@header + cmd
  end
  def createSnapshot(snapshotDir)
    snapshotName = Time.now.to_s
    cmd = "createSnapshot #{snapshotDir} #{snapshotName}"
    return @@header + cmd
  end
  def deleteSnapshot(snapshotDir,snapshotName)
    cmd = "deleteSnapshot #{snapshotDir} #{snapshotName}"
    return @@header + cmd
  end
  def df(path)
    cmd = "df -h #{path}"
    return @@header + cmd
  end
  def du(path)
    cmd = "df -s -h #{path}"
    return @@header + cmd
  end
  def help
    cmd = "help"
    return @@header + cmd
  end
  def expunge()
#	[-expunge]
#	[-find( <path> ... <expression> ...])
#	[-get [-p] [-ignoreCrc] [-crc] <src> 
#       end
#... <localdst>]	def getfacl( [-R] <path>])
#... <localdst>]	[-getfacl [-R] <path>]
  end
  def getfattr()
    #	[-getfattr [-R] {-n name | -d} [-e en] <path>]
  end
  def getmerge()
    #	[-getmerge [-nl] <src> <localdst>]
  end
  def ls(path,recur=false,dir=false)
#    [-ls [-d] [-h] [-R] [<path> ...]]
  end
=begin
	def mkdir( [-p] <path> ...])
	[-mkdir [-p] <path> ...]
      end
	def moveFromLocal( <localsrc> ... <dst>])
	[-moveFromLocal <localsrc> ... <dst>]
      end
	def moveToLocal( <src> <localdst>])
	[-moveToLocal <src> <localdst>]
      end
	def mv( <src> ... <dst>])
	[-mv <src> ... <dst>]
      end
	def put( [-f] [-p] [-l] <localsrc> ... <dst>])
	[-put [-f] [-p] [-l] <localsrc> ... <dst>]
      end
	def renameSnapshot( <snapshotDir> <oldName> <newName>])
	[-renameSnapshot <snapshotDir> <oldName> <newName>]
      end
	def rm( [-f] [-r|-R] [-skipTrash] [-safely] <src> ...])
	[-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...]
      end
	def rmdir( [--ignore-fail-on-non-empty] <dir> ...])
	[-rmdir [--ignore-fail-on-non-empty] <dir> ...]
      end
	def setfacl( [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]])
	[-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
      end
	def setfattr( {-n name [-v value] | -x name} <path>])
	[-setfattr {-n name [-v value] | -x name} <path>]
      end
	def setrep( [-R] [-w] <rep> <path> ...])
	[-setrep [-R] [-w] <rep> <path> ...]
      end
	def stat( [format] <path> ...])
	[-stat [format] <path> ...]
      end
	def tail( [-f] <file>])
	[-tail [-f] <file>]
      end
	def test( -[defsz] <path>])
	[-test -[defsz] <path>]
      end
	def text( [-ignoreCrc] <src> ...])
	[-text [-ignoreCrc] <src> ...]
      end
	def touchz( <path> ...])
	[-touchz <path> ...]
      end
	def truncate( [-w] <length> <path> ...])
	[-truncate [-w] <length> <path> ...]
      end
=end
end

=begin



-df [-h] [<path> ...] :
  Shows the capacity, free and used space of the filesystem. If the filesystem has
  multiple partitions, and no path to a particular partition is specified, then
  the status of the root partitions will be shown.
                                                                                 
  -h  Formats the sizes of files in a human-readable fashion rather than a number
      of bytes.                                                                  

-du [-s] [-h] <path> ... :
  Show the amount of space, in bytes, used by the files that match the specified
  file pattern. The following flags are optional:
                                                                                 
  -s  Rather than showing the size of each individual file that matches the      
      pattern, shows the total (summary) size.                                   
  -h  Formats the sizes of files in a human-readable fashion rather than a number
      of bytes.                                                                  
  
  Note that, even without the -s option, this only shows size summaries one level
  deep into a directory.
  
  The output is in the form 
  	size	name(full path)

-expunge :
  Empty the Trash

-find <path> ... <expression> ... :
  Finds all files that match the specified expression and
  applies selected actions to them. If no <path> is specified
  then defaults to the current working directory. If no
  expression is specified then defaults to -print.
  
  The following primary expressions are recognised:
    -name pattern
    -iname pattern
      Evaluates as true if the basename of the file matches the
      pattern using standard file system globbing.
      If -iname is used then the match is case insensitive.
  
    -print
    -print0
      Always evaluates to true. Causes the current pathname to be
      written to standard output followed by a newline. If the -print0
      expression is used then an ASCII NULL character is appended rather
      than a newline.
  
  The following operators are recognised:
    expression -a expression
    expression -and expression
    expression expression
      Logical AND operator for joining two expressions. Returns
      true if both child expressions return true. Implied by the
      juxtaposition of two expressions and so does not need to be
      explicitly specified. The second expression will not be
      applied if the first fails.

-get [-p] [-ignoreCrc] [-crc] <src> ... <localdst> :
  Copy files that match the file pattern <src> to the local name.  <src> is kept. 
  When copying multiple files, the destination must be a directory. Passing -p
  preserves access and modification times, ownership and the mode.

-getfacl [-R] <path> :
  Displays the Access Control Lists (ACLs) of files and directories. If a
  directory has a default ACL, then getfacl also displays the default ACL.
                                                                  
  -R      List the ACLs of all files and directories recursively. 
  <path>  File or directory to list.                              

-getfattr [-R] {-n name | -d} [-e en] <path> :
  Displays the extended attribute names and values (if any) for a file or
  directory.
                                                                                 
  -R             Recursively list the attributes for all files and directories.  
  -n name        Dump the named extended attribute value.                        
  -d             Dump all extended attribute values associated with pathname.    
  -e <encoding>  Encode values after retrieving them.Valid encodings are "text", 
                 "hex", and "base64". Values encoded as text strings are enclosed
                 in double quotes ("), and values encoded as hexadecimal and     
                 base64 are prefixed with 0x and 0s, respectively.               
  <path>         The file or directory.                                          

-getmerge [-nl] <src> <localdst> :
  Get all the files in the directories that match the source file pattern and
  merge and sort them to only one file on local fs. <src> is kept.
                                                        
  -nl  Add a newline character at the end of each file. 

-help [cmd ...] :
  Displays help for given command or all commands if none is specified.

-ls [-d] [-h] [-R] [<path> ...] :
  List the contents that match the specified file pattern. If path is not
  specified, the contents of /user/<currentUser> will be listed. Directory entries
  are of the form:
  	permissions - userId groupId sizeOfDirectory(in bytes)
  modificationDate(yyyy-MM-dd HH:mm) directoryName
  
  and file entries are of the form:
  	permissions numberOfReplicas userId groupId sizeOfFile(in bytes)
  modificationDate(yyyy-MM-dd HH:mm) fileName
                                                                                 
  -d  Directories are listed as plain files.                                     
  -h  Formats the sizes of files in a human-readable fashion rather than a number
      of bytes.                                                                  
  -R  Recursively list the contents of directories.                              

-mkdir [-p] <path> ... :
  Create a directory in specified location.
                                                  
  -p  Do not fail if the directory already exists 

-moveFromLocal <localsrc> ... <dst> :
  Same as -put, except that the source is deleted after it's copied.

-moveToLocal <src> <localdst> :
  Not implemented yet

-mv <src> ... <dst> :
  Move files that match the specified file pattern <src> to a destination <dst>. 
  When moving multiple files, the destination must be a directory.

-

-renameSnapshot <snapshotDir> <oldName> <newName> :
  Rename a snapshot from oldName to newName

-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ... :
  Delete all files that match the specified file pattern. Equivalent to the Unix
  command "rm <src>"
                                                                                 
  -f          If the file does not exist, do not display a diagnostic message or 
              modify the exit status to reflect an error.                        
  -[rR]       Recursively deletes directories.                                   
  -skipTrash  option bypasses trash, if enabled, and immediately deletes <src>.  
  -safely     option requires safety confirmation?if enabled, requires           
              confirmation before deleting large directory with more than        
              <hadoop.shell.delete.limit.num.files> files. Delay is expected when
              walking over large directory recursively to count the number of    
              files to be deleted before the confirmation.                       

-rmdir [--ignore-fail-on-non-empty] <dir> ... :
  Removes the directory entry specified by each directory argument, provided it is
  empty.

-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>] :
  Sets Access Control Lists (ACLs) of files and directories.
  Options:
                                                                                 
  -b          Remove all but the base ACL entries. The entries for user, group   
              and others are retained for compatibility with permission bits.    
  -k          Remove the default ACL.                                            
  -R          Apply operations to all files and directories recursively.         
  -m          Modify ACL. New entries are added to the ACL, and existing entries 
              are retained.                                                      
  -x          Remove specified ACL entries. Other ACL entries are retained.      
  --set       Fully replace the ACL, discarding all existing entries. The        
              <acl_spec> must include entries for user, group, and others for    
              compatibility with permission bits.                                
  <acl_spec>  Comma separated list of ACL entries.                               
  <path>      File or directory to modify.                                       

-setfattr {-n name [-v value] | -x name} <path> :
  Sets an extended attribute name and value for a file or directory.
                                                                                 
  -n name   The extended attribute name.                                         
  -v value  The extended attribute value. There are three different encoding     
            methods for the value. If the argument is enclosed in double quotes, 
            then the value is the string inside the quotes. If the argument is   
            prefixed with 0x or 0X, then it is taken as a hexadecimal number. If 
            the argument begins with 0s or 0S, then it is taken as a base64      
            encoding.                                                            
  -x name   Remove the extended attribute.                                       
  <path>    The file or directory.                                               

-setrep [-R] [-w] <rep> <path> ... :
  Set the replication level of a file. If <path> is a directory then the command
  recursively changes the replication factor of all files under the directory tree
  rooted at <path>.
                                                                                 
  -w  It requests that the command waits for the replication to complete. This   
      can potentially take a very long time.                                     
  -R  It is accepted for backwards compatibility. It has no effect.              

-stat [format] <path> ... :
  Print statistics about the file/directory at <path>
  in the specified format. Format accepts filesize in
  blocks (%b), type (%F), group name of owner (%g),
  name (%n), block size (%o), replication (%r), user name
  of owner (%u), modification date (%y, %Y).
  %y shows UTC date as "yyyy-MM-dd HH:mm:ss" and
  %Y shows milliseconds since January 1, 1970 UTC.
  If the format is not specified, %y is used by default.

-tail [-f] <file> :
  Show the last 1KB of the file.
                                             
  -f  Shows appended data as the file grows. 

-test -[defsz] <path> :
  Answer various questions about <path>, with result via exit status.
    -d  return 0 if <path> is a directory.
    -e  return 0 if <path> exists.
    -f  return 0 if <path> is a file.
    -s  return 0 if file <path> is greater than zero bytes in size.
    -z  return 0 if file <path> is zero bytes in size, else return 1.

-text [-ignoreCrc] <src> ... :
  Takes a source file and outputs the file in text format.
  The allowed formats are zip and TextRecordInputStream and Avro.

-touchz <path> ... :
  Creates a file of zero length at <path> with current time as the timestamp of
  that <path>. An error is returned if the file exists with non-zero length

-truncate [-w] <length> <path> ... :
  Truncate all files that match the specified file pattern to the specified
  length.
                                                                                 
  -w  Requests that the command wait for block recovery to complete, if          
      necessary.                                                                 

-usage [cmd ...] :
  Displays the usage for given command or all commands if none is specified.

Generic options supported are
-conf <configuration file>     specify an application configuration file
-D <property=value>            use value for given property
-fs <local|namenode:port>      specify a namenode
-jt <local|resourcemanager:port>    specify a ResourceManager
-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster
-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath.
-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines.

The general command line syntax is
bin/hadoop command [genericOptions] [commandOptions]
=end

  class SSH_hdp
    def initialize
      @host = '127.0.0.1:12341'
      @user = 'userooneusertwo'
      @password = 'password'
      @hdp_control = Hadoop_FS.new
      @commands = []
      @hdp_controllers = {'hdpfs'=>Hadoop_FS,'sqoop'=>Sqoop}
    end
    attr_accessor :host, :user, :password, :commands
    def hdp_control=(a)
      @hdp_control = @hdp_controllers[a].new
    end
    def cmds_purge
      @commands = []
    end
    def cmd_to_s(cmd)
      if @hdp_control.respond_to?(cmd.at(0)) then
        if cmd.at(1).empty? then
          ret = "\tresult = ssh.exec!('#{@hdp_control.method(cmd.at(0)).call}')\n\tputs result"
        else
          ret = "\tresult = ssh.exec!('#{@hdp_control.method(cmd.at(0)).call(cmd.at(1).join(','))}')\n\tputs result"  
        end
      else
        ret = "error: #{@hdp_control.class} does not respond to #{cmd}!"
      end
      ret
    end
    def script
      body = ''
      header = "require 'net/ssh'\n"
      header += "Net::SSH.start('#{host}', '#{user}', :password => '#{password}' ) {|ssh|\n"
      @commands.each{|c|
        body += cmd_to_s(c)
        body += "\n"
      }
      body += " }\n"
      return header + body
    end
    def add_cmd(a)
      tmp1 = []
      tmp = a.split(' ')
      tmp1 << tmp.at(0)
      tmp.shift
      tmp1 << tmp
      @commands << tmp1
    end
  end
  class Sqoop
    def initialize
      @jdbc_conn ='jdbc:mysql://database.example.com/employees'
      @db_ms = 'mySql'
      @db_host = 'database.example.com'
      @db_name = 'employees'
      @db_user = 'a_person'
      @password = 'a_password' #better with --password_file  !!!
      @hdfs_dir = '/user/foo/results'
#      raise "HDFS URL must be a hdfs or s3 resource" unless ['hdfs','s3'].include?(@hdfs_dir.split(':')[0].downcase)
      @hdfs_url = hdfs_url
      @db_options = " --connect jdbc:#{@db_ms.downcase}://#{db_host}/#{db_name} --username #{db_user} --password #{password}"
    end
    attr_accessor :jdbc_conn, :db_ms, :db_host, :db_name, :db_user, :password, :hdfs_url
    def test_db
      output = "sqoop list-databases" + @db_options
      raise "Database connection failed" if output.nil?
      output
    end
    def import_table(*table)
      if table != []
        sqoop_command = "sqoop import" + @db_options + " --table #{table}"
      else
        sqoop_command = "sqoop import-all-tables " + @db_options
      end
      sqoop_command += " --target-dir #{@hdfs_dir}"
      output = system(sqoop_command)
      raise "Sqoop import failed" if output.nil?
      output
    end
    def export_table(table)
      sqoop_command = "sqoop export" + @db_options + " --table #{table} --export-dir #{@hdfs_url}"
      output = system(sqoop_command)
      raise "Sqoop export failed" if output.nil?
      output  
    end
  end

  debin_int_commands = ['sudo apt-get update','\curl -L https://get.rvm.io | bash -s stable --rails,source ~/.rvm/scripts/rvm','ssh-keygen -t rsa','cat ~/.ssh/id_rsa.pub | ssh #{"userat123.45.56.78"} "mkdir -p ~/.ssh && cat >>  ~/.ssh/authorized_keys"','']

hdp_ambari_admin_commands = ['sudo ambari-admin-password-reset','sudo ambari-agent restart']
a = SSH_hdp.new
a.add_cmd('cat arg1')
a.add_cmd('help')
print a.script
a.cmds_purge
a.hdp_control = 'sqoop'
a.add_cmd('test_db')
print a.script
