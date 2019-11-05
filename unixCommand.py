import subprocess


class UnixCommand:

    def run_unix_cmd(self, args_list):
        """
        run linux commands
        """
        print('Running system command:{0}'.format(' '.join(args_list)))
        #proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        proc = subprocess.Popen(' '.join(args_list), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err
