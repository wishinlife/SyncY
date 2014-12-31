#!/usr/bin/env python
# encoding:utf-8
####################################################################################################
#
# Author: wishinlife
# QQ: 57956720
# E-Mail: wishinlife@gmail.com
# Web Home: http://syncyhome.duapp.com, http://hi.baidu.com/wishinlife
# Update date: 2014-12-20
# VERSION: 2.0.0
# Required packages: kmod-nls-utf8, libopenssl, libcurl, python, python-mini, python-curl
#
####################################################################################################

import os
import stat
import sys
import hashlib
import time
import re
import struct
import zlib
import pycurl
from urllib import quote_plus
import threading
import fcntl
# import binascii
# import fileinput

# set config_file and pidfile for your config storage path.
__CONFIG_FILE__ = '/etc/config/syncy'
__PIDFILE__ = '/var/run/syncy.pid'

#  Don't modify the following.
__VERSION__ = '2.0.0'


class SyncY:
    def __init__(self, argv=sys.argv[1:]):
        self._oldSTDERR = None
        self._oldSTDOUT = None
        self._argv = argv
        # check instance running status
        if len(self._argv) == 0 or self._argv[0] == 'compress' or self._argv[0] == 'convert':
            if os.path.exists(__PIDFILE__):
                pidh = open(__PIDFILE__, 'r')
                mypid = pidh.read()
                pidh.close()
                try:
                    os.kill(int(mypid), 0)
                except os.error:
                    pass
                else:
                    print("SyncY is running!")
                    sys.exit(0)
            pidh = open(__PIDFILE__, 'w')
            pidh.write(str(os.getpid()))
            pidh.close()
        if not (os.path.isfile(__CONFIG_FILE__)):
            sys.stderr.write('%s ERROR: Config file "%s" does not exist.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), __CONFIG_FILE__))
            sys.exit(1)
        # init global var
        self._pcsroot = '/apps/SyncY'
        self._failcount = 0
        self._syncydb = None  # syncy 同步数据文件名
        self._syncydbtmp = None  # syncy 同步数据临时文件，用于同步数据压缩
        self._sydb = None  # syncy 同步数据文件句柄
        self._sydblen = None  # syncy 同步数据文件大小
        self._syncData = None  # syncy 同步数据缓存
        self._basedirlen = None
        self._config = {
            'syncyerrlog': '',
            'syncylog'	: '',
            'blocksize'		: 10,
            'ondup'			: 'rename',
            'datacache'		: 'on',
            'slicedownload'	: 'on',   	# slice download file
            'excludefiles'	: '',
            'listnumber'	: 100,
            'retrytimes'	: 3,
            'retrydelay'	: 3,
            'maxsendspeed'	: 0,
            'maxrecvspeed'	: 0,
            'speedlimitperiod': '0-0',    # 限速时间段
            'syncperiod'	: '0-24',
            'syncinterval'	: 3600,
            'tasknumber'	: 1,
            'threadnumber'	: 2}        # 线程数量
        self._syncytoken = {'synctotal': 0}
        self._syncpath = {}

        # read config_file
        sycfg = open(__CONFIG_FILE__, 'r')
        line = sycfg.readline()
        section = ''
        while line:
            if re.findall(r'^\s*#', line) or re.findall(r'^\s*$', line):
                line = sycfg.readline()
                continue
            line = re.sub(r'#[^\']*$', '', line)
            m = re.findall(r'\s*config\s+([^\s]+).*', line)
            if m:
                section = m[0].strip('\'')
                if section == 'syncpath':
                    self._syncpath[str(len(self._syncpath))] = {}
                line = sycfg.readline()
                continue
            m = re.findall(r'\s*option\s+([^\s]+)\s+\'([^\']*)\'', line)
            if m:
                if section == 'syncy':
                    self._config[m[0][0].strip('\'')] = m[0][1]
                elif section == 'syncytoken':
                    self._syncytoken[m[0][0].strip('\'')] = m[0][1]
                elif section == 'syncpath':
                    self._syncpath[str(len(self._syncpath) - 1)][m[0][0].strip('\'')] = m[0][1]
            line = sycfg.readline()
        sycfg.close()

        # get refresh token and access token if that does not exist in config_file.
        self._config['retrytimes'] = int(self._config['retrytimes'])
        self._config['retrydelay'] = int(self._config['retrydelay'])
        self._config['maxsendspeed'] = int(self._config['maxsendspeed'])
        self._config['maxrecvspeed'] = int(self._config['maxrecvspeed'])
        if 'refresh_token' not in self._syncytoken or self._syncytoken['refresh_token'] == '' or (len(self._argv) != 0 and self._argv[0] in ["sybind", "cpbind"]):
            # get device code and user_code if that does not exist in config_file.
            syutil = SYUtil(self._config, self._syncytoken)
            if (('device_code' not in self._syncytoken or self._syncytoken['device_code'] == '') and len(self._argv) == 0) or (len(self._argv) != 0 and self._argv[0] == "sybind"):
                http_code, response_str = syutil.request('https://syncyhome.duapp.com/syserver', 'method=bind_device&scope=basic,netdisk', 'POST', 'normal')
                if http_code != 200:
                    sys.stderr.write("%s ERROR: Get device code failed, %s.\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
                    sys.exit(1)
                m = re.findall(r'.*\"device_code\":\"([0-9a-z]+)\".*', response_str)
                if m:
                    device_code = m[0]
                    m = re.findall(r'.*\"user_code\":\"([0-9a-z]+)\".*', response_str)
                    user_code = m[0]
                else:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + " Can't get device code.")
                    sys.exit(1)
                if len(self._argv) != 0 and self._argv[0] == "sybind":
                    sybind = open("/tmp/syncy.bind", 'w')
                    sybind.write('{"user_code":"%s","device_code":"%s","time":%d}' % (user_code, device_code, int(time.time())))
                    sybind.close()
                    sys.exit(0)
                self._syncytoken['device_code'] = device_code
                print("Device binding Guide:")  # "\033[34mInfo
                print("     1. Open web browser to visit:\"https://openapi.baidu.com/device\" and input user code to binding your baidu account.")
                print(" ")
                print("     2. User code:\033[31m %s\033[0m" % user_code)
                print("     (The user code is available for 30 minutes.)")  # \033[0m
                print(" ")
                raw_input('     3. After granting access to the application, come back here and press [Enter] to continue.')
                print(" ")
            if len(self._argv) != 0 and self._argv[0] == "cpbind":
                sybind = open("/tmp/syncy.bind", 'r')
                bindinfo = sybind.read()
                sybind.close()
                m = re.findall(r'.*\"device_code\":\"([0-9a-z]+)\".*', bindinfo)
                os.remove("/tmp/syncy.bind")
                if m:
                    self._syncytoken['device_code'] = m[0]
                    m = re.findall(r'.*\"time\":([0-9]+).*', bindinfo)
                    if int(time.time()) - int(m[0]) >= 1800:
                        sys.exit(1)
                else:
                    sys.exit(1)
            http_code, response_str = syutil.request('https://syncyhome.duapp.com/syserver', 'method=get_device_token&code=%s' % (self._syncytoken['device_code']), 'POST', 'normal')
            if http_code != 200 or response_str == '':
                sys.stderr.write("%s ERROR: Get device token failed, error message: %s.\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
                sys.exit(1)
            m = re.findall(r'.*\"refresh_token\":\"([^"]+)\".*', response_str)
            if m:
                self._syncytoken['refresh_token'] = m[0]
                m = re.findall(r'.*\"access_token\":\"([^"]+)\".*', response_str)
                self._syncytoken['access_token'] = m[0]
                m = re.findall(r'.*\"expires_in\":([0-9]+).*', response_str)
                self._syncytoken['expires_in'] = m[0]
                self._syncytoken['refresh_date'] = int(time.time())
                self._syncytoken['compress_date'] = int(time.time())
                self.__save_config()
                if len(self._argv) != 0 and self._argv[0] == "cpbind":
                    sys.exit(0)
                print("%s Get device token success.\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            else:
                sys.stderr.write("%s ERROR: Get device token failed, error message: %s.\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
                sys.exit(1)
        if self._config['syncyerrlog'] != '' and os.path.exists(os.path.dirname(self._config['syncyerrlog'])):
            if os.path.exists(self._config['syncyerrlog']) and os.path.isdir(self._config['syncyerrlog']):
                self._config['syncyerrlog'] += 'syncyerr.log'
                self.__save_config()
            self._oldSTDERR = sys.stderr
            sys.stderr = open(self._config['syncyerrlog'], 'a', 0)
        if self._config['syncylog'] != '' and os.path.exists(os.path.dirname(self._config['syncylog'])):
            if os.path.exists(self._config['syncylog']) and os.path.isdir(self._config['syncylog']):
                self._config['syncylog'] += 'syncy.log'
                self.__save_config()
            print('%s Running log output to log file:%s.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self._config['syncylog']))
            self._oldSTDOUT = sys.stdout
            sys.stdout = open(self._config['syncylog'], 'a', 0)

        # check config settings
        self._config['blocksize'] = int(self._config['blocksize'])
        self._config['listnumber'] = int(self._config['listnumber'])
        self._config['syncinterval'] = int(self._config['syncinterval'])
        self._config['threadnumber'] = int(self._config['threadnumber'])
        self._config['tasknumber'] = int(self._config['tasknumber'])
        self._syncytoken['refresh_date'] = int(self._syncytoken['refresh_date'])
        self._syncytoken['expires_in'] = int(self._syncytoken['expires_in'])
        self._syncytoken['compress_date'] = int(self._syncytoken['compress_date'])
        self._syncytoken['synctotal'] = int(self._syncytoken['synctotal'])
        if self._config['blocksize'] < 1:
            self._config['blocksize'] = 10
            print('%s WARNING: "blocksize" must great than or equal to 1(M), set to default 10(M).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['ondup'] != 'overwrite' and self._config['ondup'] != 'rename':
            self._config['ondup'] = 'rename'
            print('%s WARNING: ondup is invalid, set to default(overwrite).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['datacache'] != 'on' and self._config['datacache'] != 'off':
            self._config['datacache'] = 'on'
            print('%s WARNING: "datacache" is invalid, set to default(on).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['slicedownload'] != 'on' and self._config['slicedownload'] != 'off':
            self._config['slicedownload'] = 'on'
            print('%s WARNING: "slicedownload" is invalid, set to default(on).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['retrytimes'] < 0:
            self._config['retrytimes'] = 3
            print('%s WARNING: "retrytimes" is invalid, set to default(3 times).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['retrydelay'] < 0:
            self._config['retrydelay'] = 3
            print('%s WARNING: "retrydelay" is invalid, set to default(3 second).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['listnumber'] < 1:
            self._config['listnumber'] = 100
            print('%s WARNING: "listnumber" must great than or equal to 1, set to default 100.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['syncinterval'] < 1:
            self._config['syncinterval'] = 3600
            print('%s WARNING: "syncinterval" must great than or equal to 1, set to default 3600.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['maxsendspeed'] < 0:
            self._config['maxsendspeed'] = 0
            print('%s WARNING: "maxsendspeed" must great than or equal to 0, set to default 0.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['maxrecvspeed'] < 0:
            self._config['maxrecvspeed'] = 0
            print('%s WARNING: "maxrecvspeed" must great than or equal to 0, set to default 100.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['threadnumber'] < 1:
            self._config['threadnumber'] = 2
            print('%s WARNING: "threadnumber" must great than or equal to 1, set to default 2.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        if self._config['tasknumber'] < 1:
            self._config['tasknumber'] = 2
            print('%s WARNING: "tasknumber" must great than or equal to 1, set to default 1.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

        # init exclude files
        exfiles = self._config['excludefiles']
        exfiles = exfiles.replace(".", "\.").replace("*", ".*").replace("?", ".?")
        self._excludefiles = exfiles.split(';')
        for i in xrange(len(self._excludefiles)):
            self._excludefiles[i] = re.compile(eval('r"^' + self._excludefiles[i] + '$"'))
        self._excludefiles.append(re.compile(r'^.*\.tmp\.syy$'))
        self._excludefiles.append(re.compile(r'^.*\.part\.syy$'))

        self._syutil = SYUtil(self._config, self._syncytoken)
        # check expiring date of access code and update.
        if (self._syncytoken['refresh_date'] + self._syncytoken['expires_in'] - 864000) < int(time.time()):
            self.__check_expires()

    @property
    def threadnumber(self):
        return self._config['threadnumber']
    @property
    def tasknumber(self):
        return self._config['tasknumber']

    def __del__(self):
        if None != self._oldSTDERR:
            sys.stderr.close()
            sys.stderr = self._oldSTDERR
        if None != self._oldSTDOUT:
            sys.stdout.close()
            sys.stdout = self._oldSTDOUT
        if os.path.exists(__PIDFILE__):
            pidh = open(__PIDFILE__, 'r')
            lckpid = pidh.read()
            pidh.close()
            if os.getpid() == int(lckpid):
                os.remove(__PIDFILE__)

    def __init_syncdata(self):
        self._syncData = {}
        if os.path.exists(self._syncydb):
            sydb = open(self._syncydb, 'rb')
            try:
                fcntl.flock(sydb, fcntl.LOCK_SH)
                dataline = sydb.read(40)
                while dataline:
                    self._syncData[dataline[24:]] = dataline[0:24]
                    dataline = sydb.read(40)
            finally:
                sydb.close()

    def __check_expires(self):
        # check update
        http_code, response_str = self._syutil.request('https://openapi.baidu.com/rest/2.0/passport/users/getLoggedInUser', 'access_token=%s' % self._syncytoken['access_token'], 'POST', 'normal')
        m = self._syutil.re['uid'].findall(response_str)
        if m:
            http_code = self._syutil.request('https://syncyhome.duapp.com/syserver', 'method=get_last_version&edition=python&ver=%s&uid=%s' % (__VERSION__, m[0]), 'POST', 'normal')
        if http_code == 200 and response_str.find('#') > -1:
            (lastver, smessage) = response_str.strip('\n').split('#')
            if lastver != __VERSION__:
                sys.stderr.write('%s %s\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), smessage))
                print('%s %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), smessage))
        if (self._syncytoken['refresh_date'] + self._syncytoken['expires_in'] - 864000) > int(time.time()):
            return
        # refresh access token
        http_code, response_str = self._syutil.request('https://syncyhome.duapp.com/syserver', 'method=refresh_access_token&refresh_token=%s' % (self._syncytoken['refresh_token']), 'POST', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Refresh access token failed: %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
            return 1
        m = re.findall(r'.*\"refresh_token\":\"([^"]+)\".*', response_str)
        if m:
            self._syncytoken['refresh_token'] = m[0]
            m = re.findall(r'.*\"access_token\":\"([^"]+)\".*', response_str)
            self._syncytoken['access_token'] = m[0]
            m = re.findall(r'.*\"expires_in\":([0-9]+).*', response_str)
            self._syncytoken['expires_in'] = int(m[0])
            self._syncytoken['refresh_date'] = int(time.time())
            self.__save_config()
            self._syutil.setToken(self._syncytoken)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' Refresh access token success.')
            return 0
        else:
            sys.stderr.write('%s ERROR: Refresh access token failed: %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
            return 1

    def __save_config(self):
        sycfg = open(__CONFIG_FILE__ + '.sybak', 'w')
        sycfg.write("\nconfig syncy\n")
        for key, value in self._config.items():
            sycfg.write("\toption " + key + " '" + str(value) + "'\n")
        sycfg.write("\nconfig syncytoken\n")
        for key, value in self._syncytoken.items():
            sycfg.write("\toption " + key + " '" + str(value) + "'\n")
        for i in range(len(self._syncpath)):
            sycfg.write("\nconfig syncpath\n")
            for key, value in self._syncpath[str(i)].items():
                sycfg.write("\toption " + key + " '" + str(value) + "'\n")
        sycfg.close()
        pmeta = os.stat(__CONFIG_FILE__)
        os.rename(__CONFIG_FILE__ + '.sybak', __CONFIG_FILE__)
        os.lchown(__CONFIG_FILE__, pmeta.st_uid, pmeta.st_gid)
        os.chmod(__CONFIG_FILE__, pmeta.st_mode)

    @staticmethod
    def __catpath(*names):
        fullpath = '/'.join(names)
        fullpath = re.sub(r'/+', '/', fullpath)
        fullpath = re.sub(r'/$', '', fullpath)
        return fullpath

    def __upload_file_nosync(self, filepath, pcspath):
        #  don't record sync info.
        #  filepath	: local file full path
        #  pcspath	: pcs full path
        uripath = quote_plus(pcspath)
        http_code, response_str = self._syutil.request('https://c.pcs.baidu.com/rest/2.0/pcs/file?method=upload&access_token=%s&path=%s&ondup=newcopy' % (self._syncytoken['access_token'], uripath), '', 'POST', 'upfile', filepath)
        if http_code != 200:
            sys.stderr.write('%s ERROR: Upload file to pcs failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, filepath, response_str))
            # self.__rm_pcsfile(pcspath,'s')
            return 1
        print('%s Upload file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), filepath))
        return 0

    def __rm_localfile(self, delpath, slient=''):
        try:
            if os.path.isfile(delpath):
                os.remove(delpath)
                if slient == '':
                    print('%s Delete local file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), delpath))
            elif os.path.isdir(delpath):
                fnlist = os.listdir(delpath)
                for i in xrange(len(fnlist)):
                    self.__rm_localfile(delpath + '/' + fnlist[i])
                os.rmdir(delpath)
                print('%s Delete local directory "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), delpath))
        except os.error:
            sys.stderr.write('%s Delete local directory "%s" failed.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), delpath))
            return 1
        return 0

    def __compress_data(self, pathname, sydbnew, sydb=None, sydblen=0):
        fnlist = os.listdir(pathname)
        fnlist.sort()
        for fnname in fnlist:
            if fnname[0:1] == '.':
                continue
            fullpath = pathname + '/' + fnname
            if os.path.isdir(fullpath):
                if self._config['datacache'] == 'on':
                    self.__compress_data(fullpath, sydbnew)
                else:
                    self.__compress_data(fullpath, sydbnew, sydb, sydblen)
            elif os.path.isfile(fullpath):
                fnstat = os.stat(fullpath)
                md5 = hashlib.md5(fullpath[self._basedirlen:] + '\n').digest()
                prk = struct.pack('>I', int(fnstat.st_mtime)) + struct.pack('>I', fnstat.st_size % 4294967296)
                if self._config['datacache'] == 'on':
                    if md5 in self._syncData and self._syncData[md5][16:]:
                        sydbnew.write(self._syncData[md5] + md5)
                        del self._syncData[md5]
                else:
                    if sydb.tell() == sydblen:
                        sydb.seek(0)
                    datarec = sydb.read(40)
                    readlen = 40
                    while datarec and readlen <= sydblen:
                        if datarec[16:] == prk + md5:
                            sydbnew.write(datarec)
                            break
                        if readlen == sydblen:
                            break
                        if sydb.tell() == sydblen:
                            sydb.seek(0)
                        datarec = sydb.read(40)
                        readlen += 40
        return 0

    def __start_compress(self, pathname=''):
        if pathname == '':  # 压缩所有同步类型不为sync的同步信息数据 ,sync类型的必须在正确同步完成之后才可以执行压缩
            mpath = []
            for i in range(len(self._syncpath)):
                if self._syncpath[str(i)]['synctype'].lower() not in ['4', 's', 'sync']:
                    mpath.append(self._syncpath[str(i)]['localpath'])
            print("%s Start compress sync data." % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        else:
            mpath = [pathname]
        for ipath in mpath:
            if ipath == '':
                continue
            self._basedirlen = len(ipath)
            self._syncydb = ipath + '/.syncy.info.db'
            if os.path.exists(self._syncydb):
                self._syncydbtmp = ipath + '/.syncy.info.db1'
                if os.path.exists(self._syncydbtmp):
                    os.remove(self._syncydbtmp)
                sydbnew = open(self._syncydbtmp, 'wb')
                if self._config['datacache'] == 'on':
                    self.__init_syncdata()
                    self.__compress_data(ipath, sydbnew)
                    del self._syncData
                else:
                    sydb = open(self._syncydb, 'rb')
                    sydblen = os.stat(self._syncydb).st_size
                    self.__compress_data(ipath, sydbnew, sydb, sydblen)
                    sydb.close()
                sydbnew.flush()     # v2.0新增
                os.fdatasync(sydbnew)  # v2.0新增
                sydbnew.close()
                os.rename(self._syncydbtmp, self._syncydb)
                # dirfd = os.open(os.path.dirname(self._syncydb), os.O_DIRECTORY)  # v2.0新增
                # os.fsync(dirfd)  # v2.0新增
                # os.close(dirfd)  # v2.0新增
        if pathname == '':
            self._syncytoken['compress_date'] = int(time.time())
            self._syncytoken['synctotal'] = 0
            self.__save_config()
            print("%s Sync data compress completed." % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    def __check_excludefiles(self, filepath):
        #  filepath	: local or remote full path
        for reexf in self._excludefiles:
            if reexf.findall(filepath):
                return 1
        return 0

    def __check_syncstatus(self, rmd5, fmtime, fsize, fmd5):
        #  rmd5		: remote file mtime(md5)
        #  fmtime	: local file mtime
        #  fsize	: local file size
        #  fmd5		: md5 of local file full path
        if rmd5 != '*':
            rmd5 = rmd5.decode('hex')
        if fmtime != '*':
            fmtime = struct.pack('>I', fmtime)
        fsize = struct.pack('>I', fsize % 4294967296)
        if self._config['datacache'] == 'on':
            if fmd5 not in self._syncData:
                return 0
            if rmd5 == '*' and self._syncData[fmd5][16:] == fmtime + fsize:  # 本地未修改
                return 1
            elif fmtime == '*' and self._syncData[fmd5][0:16] + self._syncData[fmd5][20:] == rmd5 + fsize:  # 远程未修改
                return 1
            elif self._syncData[fmd5] == rmd5 + fmtime + fsize:  # 本地和远程都没有变化
                return 1
        else:
            if self._sydb.tell() == self._sydblen:
                self._sydb.seek(0)
            datarec = self._sydb.read(40)
            readlen = 40
            while datarec and readlen <= self._sydblen:
                if rmd5 == '*' and datarec[16:] == fmtime + fsize + fmd5:
                    return 1
                elif fmtime == '*' and datarec[0:16] + datarec[20:] == rmd5 + fsize + fmd5:
                    return 1
                elif datarec == rmd5 + fmtime + fsize + fmd5:
                    return 1
                if readlen == self._sydblen:
                    break
                if self._sydb.tell() == self._sydblen:
                    self._sydb.seek(0)
                datarec = self._sydb.read(40)
                readlen += 40
        return 0

    def __syncy_upload(self, ldir, rdir):
        #  ldir: local full path(folder);
        #  rdir: remote full path(folder).
        fnlist = os.listdir(ldir)
        fnlist.sort()
        for fi in xrange(len(fnlist)):
            lfullpath = ldir + '/' + fnlist[fi]
            if fnlist[fi][0:1] == '.' or self.__check_excludefiles(lfullpath) == 1 or self._syutil.check_pcspath(rdir, fnlist[fi]) == 1:
                continue
            rfullpath = rdir + '/' + fnlist[fi]
            if os.path.isdir(lfullpath):
                self.__syncy_upload(lfullpath, rfullpath)
            else:
                fmeta = os.stat(lfullpath)
                fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                if self.__check_syncstatus('*', int(fmeta.st_mtime), fmeta.st_size, fnmd5) == 0:
                    if self._config['ondup'] == 'rename':
                        ondup = 'newcopy'
                    else:
                        ondup = 'overwrite'
                    if TaskSemaphore.acquire():
                        # argvs = (sync_op, local_file_full_path, local_file_mtime, local_file_size, md5_of_local_file_full_path, remote_full_path, remote_size, remote_file_md5, ondup)
                        argvs = ('upload', lfullpath, int(fmeta.st_mtime), fmeta.st_size, fnmd5, rfullpath, 0, 0, ondup)
                        syncthread = SYTrans(self._config, self._syncytoken, self._syncydb, argvs)
                        syncthread.start()
                else:
                    continue
        return 0

    def __syncy_uploadplus(self, ldir, rdir):
        #  ldir: local full path(folder);
        #  rdir: remote full path(folder).
        startidx = 0
        retcode, rfnlist = self._syutil.get_pcs_filelist(rdir, startidx, self._config['listnumber'])
        if retcode != 0 and retcode != 31066:
            self._failcount += 1
            return 1
        lfnlist = os.listdir(ldir)
        lfnlist.sort()
        while retcode == 0:
            for i in xrange(len(rfnlist)):
                rfullpath = self._re['path'].findall(rfnlist[i])[0]
                fnname = os.path.basename(rfullpath)
                lfullpath = ldir + '/' + fnname
                if self.__check_excludefiles(lfullpath) == 1:
                    continue
                if os.path.exists(lfullpath):
                    for idx in xrange(len(lfnlist)):  # delete item from local file list
                        if lfnlist[idx] == fnname:
                            del lfnlist[idx]
                            break
                else:
                    continue
                fnisdir = self._re['isdir'].findall(rfnlist[i])[0]
                if (fnisdir == '1' and os.path.isfile(lfullpath)) or (fnisdir == 0 and os.path.isdir(lfullpath)):
                    if self._config['ondup'] == 'rename':  #  本地和远程文件类型不同，重命名或删除远程文件
                        fnnamenew = rdir + '/' + self.__get_newname(fnname)
                        if len(fnnamenew) >= 1000:
                            sys.stderr.write('%s ERROR: Rename faild, the length of PCS path "%s" must less than 1000, skip upload "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), fnnamenew, lfullpath))
                            self._failcount += 1
                            continue
                        if self.__mv_pcsfile(rfullpath, fnnamenew) == 1:
                            sys.stderr.write('%s ERROR: Rename "%s" failed, skip upload "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), rfullpath, lfullpath))
                            self._failcount += 1
                            continue
                    else:
                        self.__rm_pcsfile(rfullpath, 's')
                    if os.path.isdir(lfullpath):
                        self.__syncy_uploadplus(lfullpath, rfullpath)
                        continue
                    else:
                        fmeta = os.stat(lfullpath)
                        fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                        ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                elif fnisdir == '1':
                    self.__syncy_uploadplus(lfullpath, rfullpath)
                    continue
                else:  #  远程和本地都是普通文件
                    fmeta = os.stat(lfullpath)
                    fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                    rmd5 = self._re['md5'].findall(rfnlist[i])[0]
                    rsize = int(self._re['size'].findall(rfnlist[i])[0])
                    if fmeta.st_size == rsize:
                        if self.__check_syncstatus(rmd5, int(fmeta.st_mtime), rsize, fnmd5) == 1:
                            continue
                    if self._config['ondup'] == 'rename':
                        fnnamenew = rdir + '/' + self.__get_newname(fnname)
                        if len(fnnamenew) >= 1000:
                            sys.stderr.write('%s ERROR: Rename faild, the length of PCS path "%s" must less than 1000, skip upload "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), fnnamenew, lfullpath))
                            self._failcount += 1
                            continue
                        if self.__mv_pcsfile(rfullpath, fnnamenew) == 1:
                            sys.stderr.write('%s ERROR: Rename "%s" failed, skip upload "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), rfullpath, lfullpath))
                            self._failcount += 1
                            continue
                    else:
                        self.__rm_pcsfile(rfullpath, 's')
                    if os.path.exists(lfullpath + '.tmp.syy'):
                        ret = self.__slice_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                    else:
                        ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                if ret == 0:
                    self._synccount += 1
                else:
                    self._failcount += 1
            if len(rfnlist) < self._config['listnumber']:
                break
            startidx += self._config['listnumber']
            retcode, rfnlist = self.__get_pcs_filelist(rdir, startidx, startidx + self._config['listnumber'])
            if retcode != 0:
                self._failcount += 1
                return 1
        for idx in xrange(len(lfnlist)):
            lfullpath = ldir + '/' + lfnlist[idx]
            if lfnlist[idx][0:1] == '.' or self.__check_excludefiles(lfullpath) == 1 or self.__check_pcspath(rdir, lfnlist[idx]) == 1:
                continue
            rfullpath = rdir + '/' + lfnlist[idx]
            if os.path.isdir(lfullpath):
                self.__syncy_uploadplus(lfullpath, rfullpath)
            elif os.path.isfile(lfullpath):
                fmeta = os.stat(lfullpath)
                fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                if os.path.exists(lfullpath + '.tmp.syy'):
                    ret = self.__slice_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                else:
                    ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                if ret == 0:
                    self._synccount += 1
                else:
                    self._failcount += 1
        return 0

    def __syncy_download(self, ldir, rdir):
        #  ldir: local full path(folder);
        #  rdir: remote full path(folder).
        startIdx = 0
        retcode, rfnlist = self.__get_pcs_filelist(rdir, startIdx, self._config['listnumber'])
        if retcode != 0:
            return 1
        while retcode == 0:
            for i in xrange(len(rfnlist)):
                rfullpath = self._re['path'].findall(rfnlist[i])[0]
                fnname = os.path.basename(rfullpath)
                if self.__check_excludefiles(rfullpath) == 1:
                    continue
                fnisdir = self._re['isdir'].findall(rfnlist[i])[0]
                lfullpath = ldir + '/' + fnname
                if fnisdir == '1':
                    if os.path.exists(lfullpath) and os.path.isfile(lfullpath):
                        if self._config['ondup'] == 'rename':
                            fnnamenew = ldir + '/' + self.__get_newname(fnname)
                            os.rename(lfullpath, fnnamenew)
                        else:
                            self.__rm_localfile(lfullpath)
                    if not (os.path.exists(lfullpath)):
                        os.mkdir(lfullpath)
                        pmeta = os.stat(ldir)
                        os.lchown(lfullpath, pmeta.st_uid, pmeta.st_gid)
                        os.chmod(lfullpath, pmeta.st_mode)
                    self.__syncy_download(lfullpath, rfullpath)
                else:
                    rmd5 = self._re['md5'].findall(rfnlist[i])[0]
                    rsize = int(self._re['size'].findall(rfnlist[i])[0])
                    fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                    if not (os.path.exists(lfullpath + '.tmp.syy')):
                        if self.__check_syncstatus(rmd5, '*', rsize, fnmd5) == 1:
                            continue
                        if os.path.exists(lfullpath) and self._config['ondup'] == 'rename':
                            fnnamenew = ldir + '/' + self.__get_newname(fnname)
                            os.rename(lfullpath, fnnamenew)
                        elif os.path.exists(lfullpath):
                            self.__rm_localfile(lfullpath)
                    ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                    if ret == 0:
                        self._synccount += 1
                    else:
                        self._failcount += 1
            if len(rfnlist) < self._config['listnumber']:
                break
            startIdx += self._config['listnumber']
            retcode, rfnlist = self.__get_pcs_filelist(rdir, startIdx, startIdx + self._config['listnumber'])
            if retcode != 0:
                return 1
        return 0

    def __syncy_downloadplus(self, ldir, rdir):
        # ldir: local full path(folder);
        # rdir: remote full path(folder).
        startIdx = 0
        retcode, rfnlist = self.__get_pcs_filelist(rdir, startIdx, self._config['listnumber'])
        if retcode != 0:
            return 1
        while retcode == 0:
            for i in xrange(len(rfnlist)):
                rfullpath = self._re['path'].findall(rfnlist[i])[0]
                fnname = os.path.basename(rfullpath)
                if self.__check_excludefiles(rfullpath) == 1:
                    continue
                fnisdir = self._re['isdir'].findall(rfnlist[i])[0]
                lfullpath = ldir + '/' + fnname
                if fnisdir == '1':
                    if os.path.exists(lfullpath) and os.path.isfile(lfullpath):
                        if self._config['ondup'] == 'rename':
                            fnnamenew = ldir + '/' + self.__get_newname(fnname)
                            os.rename(lfullpath, fnnamenew)
                        else:
                            self.__rm_localfile(lfullpath)
                    if not (os.path.exists(lfullpath)):
                        os.mkdir(lfullpath)
                        pmeta = os.stat(ldir)
                        os.lchown(lfullpath, pmeta.st_uid, pmeta.st_gid)
                        os.chmod(lfullpath, pmeta.st_mode)
                    self.__syncy_downloadplus(lfullpath, rfullpath)
                else:
                    fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                    rmd5 = self._re['md5'].findall(rfnlist[i])[0]
                    rsize = int(self._re['size'].findall(rfnlist[i])[0])
                    if os.path.exists(lfullpath) and not (os.path.exists(lfullpath + '.tmp.syy')):
                        fmeta = os.stat(lfullpath)
                        if self.__check_syncstatus(rmd5, int(fmeta.st_mtime), rsize, fnmd5) == 1:
                            continue
                        if self._config['ondup'] == 'rename':
                            fnnamenew = ldir + '/' + self.__get_newname(fnname)
                            os.rename(lfullpath, fnnamenew)
                        else:
                            self.__rm_localfile(lfullpath)
                    ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                    if ret == 0:
                        self._synccount += 1
                    else:
                        self._failcount += 1
            if len(rfnlist) < self._config['listnumber']:
                break
            startIdx += self._config['listnumber']
            retcode, rfnlist = self.__get_pcs_filelist(rdir, startIdx, startIdx + self._config['listnumber'])
            if retcode != 0:
                return 1
        return 0

    def __syncy_sync(self, ldir, rdir):
        # ldir: local full path(folder);
        # rdir: remote full path(folder).
        startIdx = 0
        retcode, rfnlist = self._syutil.get_pcs_filelist(rdir, startIdx, self._config['listnumber'])
        if retcode != 0 and retcode != 31066:
            return 1
        lfnlist = os.listdir(ldir)
        lfnlist.sort()
        while retcode == 0:
            for i in xrange(len(rfnlist)):
                rfullpath = self._re['path'].findall(rfnlist[i])[0]
                fnname = os.path.basename(rfullpath)
                if self.__check_excludefiles(rfullpath) == 1:
                    continue
                lfullpath = ldir + '/' + fnname
                if os.path.exists(lfullpath):
                    for idx in xrange(len(lfnlist)):  # delete item from local file list
                        if lfnlist[idx] == fnname:
                            del lfnlist[idx]
                            break
                fnisdir = self._re['isdir'].findall(rfnlist[i])[0]
                rmtime = int(self._re['mtime'].findall(rfnlist[i])[0])
                if fnisdir == '1':
                    if os.path.exists(lfullpath) and os.path.isfile(lfullpath):
                        fmeta = os.stat(lfullpath)
                        fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                        if self.__check_syncstatus('*', int(fmeta.st_mtime), fmeta.st_size, fnmd5) == 1:  #上次同步过此文件，所以远端为最新
                            self.__rm_localfile(lfullpath)
                            ret = self.__syncy_downloadplus(lfullpath, rfullpath)
                            if ret == 0:
                                self._synccount += 1
                            else:
                                self._failcount += 1
                            continue
                        elif rmtime > int(fmeta.st_mtime):
                            self.__rm_localfile(lfullpath)
                            ret = self.__syncy_downloadplus(lfullpath, rfullpath)
                            if ret == 0:
                                self._synccount += 1
                            else:
                                self._failcount += 1
                            continue
                        else:
                            self.__rm_pcsfile(rfullpath, 's')
                            ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                    else:
                        if not (os.path.exists(lfullpath)):
                            os.mkdir(lfullpath)
                            pmeta = os.stat(ldir)
                            os.lchown(lfullpath, pmeta.st_uid, pmeta.st_gid)
                            os.chmod(lfullpath, pmeta.st_mode)
                        self.__syncy_sync(lfullpath, rfullpath)
                        continue
                else:
                    rmd5 = self._re['md5'].findall(rfnlist[i])[0]
                    rsize = int(self._re['size'].findall(rfnlist[i])[0])
                    fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                    if os.path.exists(lfullpath) and os.path.isdir(lfullpath):
                        if self.__check_syncstatus(rmd5, '*', rsize, fnmd5) == 1:  #上次同步过此文件，所以本地为最新
                            self.__rm_pcsfile(rfullpath, 's')
                            self.__syncy_uploadplus(lfullpath, rfullpath)
                            continue
                        else:
                            lmtime = int(os.stat(lfullpath).st_mtime)
                            if rmtime > lmtime:
                                self.__rm_localfile(lfullpath)
                                ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                            else:
                                self.__rm_pcsfile(rfullpath, 's')
                                self.__syncy_uploadplus(lfullpath, rfullpath)
                                continue
                    elif os.path.exists(lfullpath):
                        fmeta = os.stat(lfullpath)
                        if rsize == fmeta.st_size and self.__check_syncstatus(rmd5, int(fmeta.st_mtime), fmeta.st_size, fnmd5) == 1:  #同步过，并且一致
                            continue
                        elif self.__check_syncstatus('*', int(fmeta.st_mtime), fmeta.st_size, fnmd5) == 1:  # 远端被更新过，本地无变化
                            self.__rm_localfile(lfullpath)
                            ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                            if ret == 0:
                                self._synccount += 1
                            else:
                                self._failcount += 1
                            continue
                        #  本地被更新过,或本地和远端都被修改过,或上次下载未完成。(如果是上传未完成，远端列表不会有此文件，有此文件只可能是从其它途径修改过)
                        if self.__check_syncstatus(rmd5, '*', rsize, fnmd5) == 1:  #  远端未发生变化，本地被更新过
                            self.__rm_pcsfile(rfullpath, 's')
                            ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                        elif os.path.exists(lfullpath + '.tmp.syy'):
                            infoh = open(lfullpath + '.tmp.syy', 'r')
                            syyinfo = infoh.readline()
                            infoh.close()
                            if syyinfo.strip('\n') == 'download ' + rmd5 + ' ' + str(rsize):
                                ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                            else:
                                os.remove(lfullpath + '.tmp.syy')  # 上次下载未完成，远端被更新，或上次上传未完成，远端被更新，可能本地也被修改，以时间来判断
                                if rmtime > int(fmeta.st_mtime):
                                    self.__rm_localfile(lfullpath)
                                    ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                                else:
                                    self.__rm_pcsfile(rfullpath)
                                    ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                        elif rmtime > int(fmeta.st_mtime):  #  本地和远端都发生变化，也不是下载未完成的情况，以时间来判断
                            self.__rm_localfile(lfullpath)
                            ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                        else:
                            self.__rm_pcsfile(rfullpath)
                            ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                    else:
                        if self.__check_syncstatus(rmd5, '*', rsize, fnmd5) == 1:  #  远端未发生变化，本地被删除
                            ret = self.__rm_pcsfile(rfullpath)
                        else:
                            ret = self.__download_file(rfullpath, rmd5, rsize, lfullpath, fnmd5)
                if ret == 0:
                    self._synccount += 1
                else:
                    self._failcount += 1
            if len(rfnlist) < self._config['listnumber']:
                break
            startIdx += self._config['listnumber']
            retcode, rfnlist = self.__get_pcs_filelist(rdir, startIdx, startIdx + self._config['listnumber'])
            if retcode != 0:
                return 1
        for idx in xrange(len(lfnlist)):  # remote not exist this files
            lfullpath = ldir + '/' + lfnlist[idx]
            if lfnlist[idx][0:1] == '.' or self.__check_excludefiles(lfullpath) == 1 or self.__check_pcspath(rdir, lfnlist[idx]) == 1:
                continue
            rfullpath = rdir + '/' + lfnlist[idx]
            if os.path.isdir(lfullpath):
                self.__syncy_sync(lfullpath, rfullpath)
                dir_files = os.listdir(ldir)
                if len(dir_files) == 0:
                    os.rmdir(lfullpath)
            elif os.path.isfile(lfullpath):
                fmeta = os.stat(lfullpath)
                fnmd5 = hashlib.md5(lfullpath[self._basedirlen:] + '\n').digest()
                if self.__check_syncstatus('*', int(fmeta.st_mtime), fmeta.st_size, fnmd5) == 1:  #远端被删除，删除本地
                    ret = self.__rm_localfile(lfullpath)
                elif os.path.exists(lfullpath + '.tmp.syy'):
                    infoh = open(lfullpath + '.tmp.syy', 'r')
                    syyinfo = infoh.readline()
                    infoh.close()
                    if syyinfo.strip('\n') == 'upload ' + str(int(fmeta.st_mtime)) + ' ' + str(fmeta.st_size):
                        ret = self.__slice_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                    else:
                        os.remove(lfullpath + '.tmp.syy')
                        ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                else:
                    ret = self.__rapid_uploadfile(lfullpath, int(fmeta.st_mtime), fmeta.st_size, rfullpath, fnmd5, 'overwrite')
                if ret == 0:
                    self._synccount += 1
                else:
                    self._failcount += 1
        return 0

    def __start_sync(self):
        self._syutil.get_pcs_quota()
        global SYNCCOUNT
        global ERRORCOUNT
        for i in range(len(self._syncpath)):
            if 'localpath' not in self._syncpath[str(i)] or 'remotepath' not in self._syncpath[str(i)] or 'synctype' not in self._syncpath[str(i)] or 'enable' not in self._syncpath[str(i)]:
                sys.stderr.write('%s ERROR: The %d\'s of syncpath setting is invalid.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), i + 1))
                continue
            if self._syncpath[str(i)]['enable'] == '0':
                continue
            SYNCCOUNT = 0   # 多线程只能基于一个同步目录，此时只有主进程在运行，不需要加锁
            ERRORCOUNT = 0
            self._failcount = 0
            ipath = ('%s:%s:%s' % (self._syncpath[str(i)]['localpath'], self._syncpath[str(i)]['remotepath'], self._syncpath[str(i)]['synctype']))
            print('%s Start sync path: "%s".' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
            localpath = self.__catpath(self._syncpath[str(i)]['localpath'])
            remotepath = self.__catpath(self._pcsroot, self._syncpath[str(i)]['remotepath'])
            ckdir = 0
            for rdir in remotepath.split('/'):
                if re.findall(r'^[\s\.\r\n].*|.*[/<>\\|\*\?:\"].*|.*[\s\.\r\n]$', rdir):    # self._re['pcspath'].findall(rdir):
                    ckdir = 1
                    break
            if ckdir != 0:
                sys.stderr.write('%s ERROR: Sync "%s" failed, remote directory error.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
                continue
            if not (os.path.exists(localpath)):
                os.mkdir(localpath)
                pmeta = os.stat(os.path.dirname(localpath))
                os.lchown(localpath, pmeta.st_uid, pmeta.st_gid)
                os.chmod(localpath, pmeta.st_mode)
            if localpath != '' and os.path.isdir(localpath):
                self._syncydb = localpath + '/.syncy.info.db'
                if self._config['datacache'] == 'on':
                    self.__init_syncdata()
                else:
                    self._sydblen = os.stat(self._syncydb).st_size
                    self._sydb = open(self._syncydb, 'rb')
                self._basedirlen = len(localpath)
                if self._syncpath[str(i)]['synctype'].lower() in ['0', 'u', 'upload']:
                    self.__syncy_upload(localpath, remotepath)
                elif self._syncpath[str(i)]['synctype'].lower() in ['1', 'u+', 'upload+']:
                    self.__syncy_uploadplus(localpath, remotepath)
                elif self._syncpath[str(i)]['synctype'].lower() in ['2', 'd', 'download']:
                    self.__syncy_download(localpath, remotepath)
                    self._syncytoken['synctotal'] += SYNCCOUNT
                    self.__save_config()
                elif self._syncpath[str(i)]['synctype'].lower() in ['3', 'd+', 'download+']:
                    self.__syncy_downloadplus(localpath, remotepath)
                elif self._syncpath[str(i)]['synctype'].lower() in ['4', 's', 'sync']:
                    self.__syncy_sync(localpath, remotepath)
                else:
                    sys.stderr.write('%s Error: The "synctype" of "%s" is invalid, must set to [0 - 4], skiped.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
                    print('%s Error: The "synctype" of "%s" is invalid, must set to [0 - 4], skiped.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
                    continue
                if self._config['datacache'] == 'on':
                    del self._syncData
                else:
                    self._sydb.close()
                while True:
                    if threading.activeCount() > 1:     # 必须等待当前目录文件全部同步完成后才能压缩同步信息，以及进行下一个同步目录的同步
                        time.sleep(3)
                    else:
                        break
                if self._failcount == 0 and ERRORCOUNT == 0:
                    if self._syncpath[str(i)]['synctype'].lower() not in ['2', 'd', 'download']:
                        self.__start_compress(self._syncpath[str(i)]['localpath'])
                    print('%s Sync path: "%s" complete, Success sync %d files.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath, SYNCCOUNT))
                else:
                    print('%s Sync path: "%s" failed, %d files success, %d files failed, %d errors occurred.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath, SYNCCOUNT, self._failcount, ERRORCOUNT))
                    sys.stderr.write('%s ERROR: Sync path: "%s" failed, %d files success, %d files failed, %d errors occurred.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath, SYNCCOUNT, self._failcount, ERRORCOUNT))
            else:
                sys.stderr.write('%s ERROR: Sync "%s" failed, local directory is not exist or is normal file.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
                print('%s ERROR: Sync "%s" failed, local directory is not exist or is normal file.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ipath))
        self._syutil.get_pcs_quota()

    @staticmethod
    def __test_chinese(tdir=''):
        unicode_str = '\u4e2d\u6587\u8f6c\u7801\u6d4b\u8bd5'
        unicode_str = eval('u"' + unicode_str + '"')
        unicode_str = unicode_str.encode('utf8')
        chnfn = open(tdir + '/' + unicode_str, 'w')
        chnfn.write(unicode_str)
        chnfn.close()

    def __data_convert(self):
        mpath = self._config['syncpath'].split(';')
        for i in range(len(mpath)):
            if mpath[i] == '':
                continue
            localdir = mpath[i].split(':')[0:1]
            syncydb = localdir + '/.syncy.info.db'
            if os.path.exists(syncydb):
                syncydbtmp = localdir + '/.syncy.info.db1'
                if os.path.exists(syncydbtmp):
                    os.remove(syncydbtmp)
                sydb = open(syncydb, 'r')
                syncinfo = sydb.readlines()
                sydb.close()
                if len(syncinfo[0]) > 100 or len(syncinfo[0].split(' ')[0]) != 32:
                    sys.stderr.write('%s Convert sync data failed "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), mpath[i]))
                    continue
                sydbnew = open(syncydbtmp, 'wb')
                for j in xrange(len(syncinfo)):
                    rmd5, lmtime, lsize, lmd5 = syncinfo[j].split(' ')
                    rmd5 = rmd5.decode('hex')                   # 16 bytes
                    lmtime = struct.pack('>I', lmtime)          # 4 bytes
                    lsize = struct.pack('>I', lsize % 4294967296)  # 4 bytes  pack("S",int($lsize/4294967296))
                    lmd5 = lmd5.decode('hex')                   # 16 bytes
                    sydbnew.write(rmd5 + lmtime + lsize + lmd5)  # all length 40 bytes.
                sydbnew.close()
                os.rename(syncydbtmp, syncydb)

    def start(self):
        # self.__cp_pcsfile('/apps/SyncY/test.pl/1-test-14M.bin.bak','/apps/SyncY/test.pl/1-test-14M.bin')
        # self.__get_pcs_filemeta('/apps/SyncY/test.pl/1-test-14M.bin')
        # sys.stderr.write(self._response_str)
        # crc,cmd5,smd5 = self.__rapid_checkcode('/mnt/sda1/testpl测试/Captain America The First Avenger 3D Half-SBS.bak.mkv')
        # print('CRC32:%s, CMD5:%s, SMD5:%s' % (crc,cmd5,smd5))
        # crc,cmd5,smd5 = self.__rapid_checkcode('/mnt/sda1/testpl测试/Captain America The First Avenger 3D Half-SBS.mkv')
        # print('CRC32:%s, CMD5:%s, SMD5:%s' % (crc,cmd5,smd5))
        # self.__check_expires()
        # self.__save_config()
        # sys.exit(0)
        #
        if len(self._argv) == 0:
            if self._config['syncperiod'] == '':
                self.__start_sync()
            else:
                starthour, endhour = self._config['syncperiod'].split('-')
                curhour = time.localtime().tm_hour
                if starthour == '' or endhour == '' or int(starthour) < 0 or int(starthour) > 23 or int(endhour) < 0 or int(endhour) > 24 or endhour == starthour:
                    print('%s WARNING: "syncperiod" is invalid, set to default(0-24).\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
                    self._config['syncperiod'] = '0-24'
                    starthour = 0
                    endhour = 24
                starthour = int(starthour)
                endhour = int(endhour)
                while True:
                    if (endhour > starthour and starthour <= curhour < endhour) or (endhour < starthour and (curhour < starthour or curhour >= endhour)):
                        self.__start_sync()
                        self.__check_expires()
                        time.sleep(self._config['syncinterval'])
                        # if (self._syncytoken['refresh_date'] + self._syncytoken['expires_in'] - 864000) < int(time.time()):
                    else:
                        time.sleep(300)
                    curhour = time.localtime().tm_hour

        elif self._argv[0] == 'compress':
            self.__start_compress()
        elif self._argv[0] == 'convert':
            self.__data_convert()
        elif self._argv[0] == 'testchinese':
            self.__test_chinese(self._argv[1])
        elif os.path.isfile(self._argv[0]):
            fname = os.path.basename(self._argv[0])
            if len(self._argv) == 2:
                pcsdir = self.__catpath(self._pcsroot, self._argv[1])
            else:
                pcsdir = self._pcsroot
            if self._syutil.check_pcspath(pcsdir, fname) == 0:
                self.__upload_file_nosync(self._argv[0], self.__catpath(pcsdir, fname))
        elif not (self._argv[0] in ["sybind", "cpbind"]):
            print('%s Unknown command "%s"' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ' '.join(self._argv)))


class SYUtil():
    def __init__(self, syconfig=None, sytoken=None):
        self.__response_str = None
        self.__config = syconfig
        self.__syncytoken = sytoken
        self.__re = {
            'path': re.compile(r'.*\"path\":\"([^"]+)\",.*'),
            'size': re.compile(r'.*\"size\":([0-9]+),.*'),
            'md5': re.compile(r'.*\"md5\":\"([^"]+)\".*'),
            'isdir': re.compile(r'.*\"isdir\":([0-1]).*'),
            'mtime': re.compile(r'.*\"mtime\":([0-9]+).*'),
            'error_code': re.compile(r'.*\"error_code\":([0-9]+),.*'),
            'newname': re.compile(r'^(.*)(\.[^.]+)$'),
            'getlist': re.compile(r'^\{\"list\":\[(\{.*\}|)\],\"request_id\".*'),
            'listrep': re.compile(r'},\{\"fs_id'),
            'pcspath': re.compile(r'^[\s\.\r\n].*|.*[/<>\\|\*\?:\"].*|.*[\s\.\r\n]$'),
            'uid': re.compile(r'.*\"uid\":\"([0-9]+)\".*')}

    @property
    def re(self):
        return self.__re
    '''
    @re.setter
    def re(self, re):
        self.__re = re
        '''
    def setConfig(self, syconfig):
        self.__config = syconfig

    def setToken(self, sytoken):
        self.__syncytoken = sytoken

    def __write_data(self, rsp):
        # 一次请求中会存在多次调用
        # rsp: response body
        self.__response_str += rsp
        return len(rsp)

    @staticmethod
    def __write_header(rsp):
        return len(rsp)

    def __islimit(self):
        starthour, endhour = self.__config['speedlimitperiod'].split('-')
        if starthour == '' or endhour == '' or int(starthour) < 0 or int(starthour) > 23 or int(endhour) < 0 or int(endhour) > 24 or endhour == starthour:
            print('%s WARNING: "speedlimitperiod" is invalid, set to default(0-0), no limit.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            self.__config['speedlimitperiod'] = '0-0'
            starthour = 0
            endhour = 0
        starthour = int(starthour)
        endhour = int(endhour)
        curhour = time.localtime().tm_hour
        if (endhour > starthour and starthour <= curhour < endhour) or (endhour < starthour and (curhour < starthour or curhour >= endhour)):
            return True
        else:
            return False

    def request(self, url, rdata, method, rtype, fnname=''):
        # url: URL
        # rdata: Data
        # method: Request method(POST OR GET)
        # rtype: Request type(normal,upfile,downfile)
        # fnname: local path
        curl = pycurl.Curl()  # 初始化CURL句柄
        curl.setopt(pycurl.URL, url)  # 设置请求的URL
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)  # 对认证证书来源的检查
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)  # 从证书中检查SSL加密算法是否存在
        curl.setopt(pycurl.FOLLOWLOCATION, 1)  # 使用自动跳转
        curl.setopt(pycurl.CONNECTTIMEOUT, 15)  # 设置连接超时，连接建立后此设置将失效
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)  # 取消连接的最低速度（字节每秒）
        curl.setopt(pycurl.LOW_SPEED_TIME, 60)  # 取消连接的最低速度持续时间（秒）
        curl.setopt(pycurl.USERAGENT, '')
        if self.__islimit():
            curl.setopt(pycurl.MAX_SEND_SPEED_LARGE, self.__config['maxsendspeed'])  # 最大上传速度
            curl.setopt(pycurl.MAX_RECV_SPEED_LARGE, self.__config['maxrecvspeed'])  # 最大下载速度
        curl.setopt(pycurl.HEADER, 0)  # 输出返回信息至返回体中（default 0）
        retrycnt = 0
        while retrycnt <= self.__config['retrytimes']:
            try:
                self.__response_str = ''
                if rtype == 'upfile':
                    curl.setopt(pycurl.UPLOAD, 1)
                    ulfile = open(fnname, 'rb')
                    if rdata != '':
                        (foffset, flen) = rdata.split(':')
                        foffset = int(foffset)
                        flen = int(flen)
                        ulfile.seek(foffset)
                    else:
                        flen = os.stat(fnname).st_size
                    curl.setopt(pycurl.READDATA, ulfile)  # \*$uData
                    curl.setopt(pycurl.INFILESIZE, flen)  # $filesize
                    curl.setopt(pycurl.WRITEFUNCTION, self.__write_data)
                    curl.perform()
                    ulfile.close()
                elif rtype == 'downfile':
                    curl.setopt(pycurl.OPT_FILETIME, 1)
                    if os.path.exists(fnname):
                        drange = str(os.stat(fnname).st_size) + '-'
                        curl.setopt(pycurl.RANGE, drange)
                    dlfile = open(fnname, 'ab')
                    curl.setopt(pycurl.WRITEDATA, dlfile)
                    curl.perform()
                    dlfile.close()
                    filemtime = curl.getinfo(pycurl.INFO_FILETIME)
                    os.utime(fnname, (filemtime, filemtime))
                    pmeta = os.stat(os.path.dirname(fnname))
                    os.lchown(fnname, pmeta.st_uid, pmeta.st_gid)
                    os.chmod(fnname, pmeta.st_mode - stat.S_IXUSR - stat.S_IXGRP - stat.S_IXOTH)
                else:
                    curl.setopt(pycurl.CUSTOMREQUEST, method)  # 设置请求方式
                    curl.setopt(pycurl.POSTFIELDS, rdata)  # 设置提交的字符串
                    curl.setopt(pycurl.WRITEFUNCTION, self.__write_data)
                    curl.perform()
                self.__response_str = self.__response_str.strip('\n')
                http_code = curl.getinfo(pycurl.HTTP_CODE)
                # check status ,retry when happaned errors.
                if http_code < 400 or http_code == 404 or retrycnt == self.__config['retrytimes']:
                    return http_code if rtype == 'downfile' else http_code, self.__response_str
                else:
                    retrycnt += 1
                    print('%s WARNING: Request failed, wait %d seconds and try again(%d). Http(%d): %s.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__config['retrydelay'], retrycnt, http_code, self.__response_str))
                    time.sleep(self.__config['retrydelay'])
            except pycurl.error, error:
                errno, errstr = error
                if retrycnt == self.__config['retrytimes']:
                    return errno if rtype == 'downfile' else errno, self.__response_str
                else:
                    retrycnt += 1
                    print('%s WARNING: Request failed, wait %d seconds and try again(%d). Curl(%d): %s.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__config['retrydelay'], retrycnt, errno, errstr))

    def request_SD(self, url, method, fnname, filelength):
        # Only download file(slice download)
        # url: URL
        # method: Request method(POST OR GET)
        # fnname: local file full path
        # filelength: the whole file length
        curl = pycurl.Curl()  # 初始化CURL句柄
        curl.setopt(pycurl.URL, url)  # 设置请求的URL
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)  # 对认证证书来源的检查
        curl.setopt(pycurl.SSL_VERIFYHOST, 2)  # 从证书中检查SSL加密算法是否存在
        curl.setopt(pycurl.FOLLOWLOCATION, 1)  # 使用自动跳转
        curl.setopt(pycurl.CONNECTTIMEOUT, 15)  # 设置连接超时，连接建立后此设置将失效
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)  # 取消连接的最低速度（字节每秒）
        curl.setopt(pycurl.LOW_SPEED_TIME, 60)  # 取消连接的最低速度持续时间（秒）
        curl.setopt(pycurl.CUSTOMREQUEST, method)  # 设置请求方式
        curl.setopt(pycurl.USERAGENT, '')
        if self.__islimit():
            curl.setopt(pycurl.MAX_SEND_SPEED_LARGE, self.__config['maxsendspeed'])  # 最大上传速度
            curl.setopt(pycurl.MAX_RECV_SPEED_LARGE, self.__config['maxrecvspeed'])  # 最大下载速度
        curl.setopt(pycurl.HEADER, 0)  # 输出返回信息至返回体中（default 0）
        retrycnt = 0
        srange = 0
        if os.path.exists(fnname):
            srange = os.stat(fnname).st_size
        while retrycnt <= self.__config['retrytimes']:
            try:
                curl.setopt(pycurl.OPT_FILETIME, 1)
                if filelength < srange + (self.__config['blocksize'] + 1) * 1048576:  # 1048576 = 1M
                    curl.setopt(pycurl.RANGE, str(srange) + '-' + str(filelength - 1))
                else:
                    curl.setopt(pycurl.RANGE, str(srange) + '-' + str(srange + self.__config['blocksize'] * 1048576 - 1))
                dlfile = open(fnname + '.part.syy', 'wb')
                curl.setopt(pycurl.WRITEDATA, dlfile)
                curl.perform()
                dlfile.close()
                http_code = curl.getinfo(pycurl.HTTP_CODE)
                # download success!
                if http_code == 200 or http_code == 206:
                    with open(fnname, "ab") as dlfh:
                        with open(fnname + '.part.syy', "rb") as ptfh:
                            fbuffer = ptfh.read(8192)
                            while fbuffer:
                                dlfh.write(fbuffer)
                                fbuffer = ptfh.read(8192)
                            ptfh.close()
                        dlfh.close()
                    os.remove(fnname + '.part.syy')
                    srange = os.stat(fnname).st_size
                    if srange == filelength:
                        filemtime = curl.getinfo(pycurl.INFO_FILETIME)
                        os.utime(fnname, (filemtime, filemtime))
                        pmeta = os.stat(os.path.dirname(fnname))
                        os.lchown(fnname, pmeta.st_uid, pmeta.st_gid)
                        os.chmod(fnname, pmeta.st_mode - stat.S_IXUSR - stat.S_IXGRP - stat.S_IXOTH)
                        return http_code
                # check status, retry when happaned errors.
                elif http_code < 400 or retrycnt == self.__config['retrytimes']:
                    return http_code
                else:
                    retrycnt += 1
                    print('%s WARNING: Request failed, wait %d seconds and try again(%d). Http(%d).' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__config['retrydelay'], retrycnt, http_code))
                    time.sleep(self.__config['retrydelay'])
            except pycurl.error, error:
                errno, errstr = error
                if retrycnt == self.__config['retrytimes']:
                    return errno
                else:
                    retrycnt += 1
                    print('%s WARNING: Request failed, wait %d seconds and try again(%d). Curl(%d): %s.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__config['retrydelay'], retrycnt, errno, errstr))

    def rm_pcsfile(self, pcspath, slient=''):
        #  pcspath	: pcs full path
        uripath = quote_plus(pcspath)
        http_code, response_str = self.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=delete&access_token=%s&path=%s' % (self.__syncytoken['access_token'], uripath), '', 'POST', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Delete remote file failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, pcspath, response_str))
            return 1
        elif slient == '':
            print('%s Delete remote file or directory "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), pcspath))
        return 0

    def mv_pcsfile(self, oldpcspath, newpcspath):
        #  oldpcspath	: old pcs full path
        #  newpcspath	: new pcs full path
        uripaths = quote_plus(oldpcspath)
        uripathd = quote_plus(newpcspath)
        http_code, response_str = self.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=move&access_token=%s&from=%s&to=%s' % (self.__syncytoken['access_token'], uripaths, uripathd), '', 'POST', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Move remote file or directory "%s" to "%s" failed(error code:%d): %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), oldpcspath, newpcspath, http_code, response_str))
            return 1
        print('%s Move remote file or directory "%s" to "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), oldpcspath, newpcspath))
        return 0

    def cp_pcsfile(self, srcpcspath, destpcspath):
        #  srcpcspath	: source pcs full path
        #  destpcspath	: destination pcs full path
        uripaths = quote_plus(srcpcspath)
        uripathd = quote_plus(destpcspath)
        http_code, response_str = self.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=copy&access_token=%s&from=%s&to=%s' % (self.__syncytoken['access_token'], uripaths, uripathd), '', 'POST', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Copy remote file or directory "%s" to "%s" failed(error code:%d): %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), srcpcspath, destpcspath, http_code, response_str))
            return 1
        print('%s Copy remote file or directory "%s" to "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), srcpcspath, destpcspath))
        return 0

    def check_pcspath(self, pcsdirname, pcsfilename):
        # pcsdirname	: parent directory full path of 'pcsfilename' on pcs
        # pcsfilename	: current directory or file name
        if len(pcsdirname) + len(pcsfilename) + 1 >= 1000:
            sys.stderr.write('%s ERROR: Length of PCS path(%s/%s) must less than 1000, skip upload.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), pcsdirname, pcsfilename))
            return 1
        # \?|"<>:*
        if self.__re['pcspath'].findall(pcsfilename):
            sys.stderr.write('%s ERROR: PCS path(%s/%s) is invalid, please check whether special characters exists in the path, skip upload the file.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), pcsdirname, pcsfilename))
            return 1
        return 0

    def get_newname(self, oldname):
        nowtime = str(time.strftime("%Y%m%d%H%M%S", time.localtime()))
        m = self.__re['newname'].findall(oldname)
        if m:
            newname = m[0][0] + '_old_' + nowtime + m[0][1]
        else:
            newname = oldname + '_old_' + nowtime
        return newname

    def get_pcs_quota(self):
        http_code, response_str = self.request('https://pcs.baidu.com/rest/2.0/pcs/quota?method=info&access_token=%s' % (self.__syncytoken['access_token']), '', 'GET', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Get pcs quota failed(error code:%d),%s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, response_str))
            return 1
        m = re.findall(r'.*\"quota\":([0-9]+).*', response_str)
        if m:
            quota = int(m[0]) / 1024 / 1024 / 1024
            m = re.findall(r'.*\"used\":([0-9]+).*', response_str)
            used = int(m[0]) / 1024 / 1024 / 1024
            print('%s PCS quota is %dG,used %dG.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), quota, used))
            return 0
        else:
            sys.stderr.write('%s ERROR: Get pcs quota failed,%s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), response_str))
            return 1

    def get_pcs_filemeta(self, jsonstring):
        # jsonstring: pcs response file json string
        # uripath = quote_plus(filepath)
        # http_code = self.__curl_request('https://pcs.baidu.com/rest/2.0/pcs/file?method=meta&access_token=%s&path=%s' % (self.__syncytoken['access_token'], uripath),'','GET','normal')
        # if http_code != 200:
        #    sys.stderr.write('%s ERROR: Get file meta failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, filepath, self._response_str))
        #    return 1
        # return 0
        try:
            isdir = self.__re['isdir'].findall(jsonstring)[0]
            fullpath = self.__re['path'].findall(jsonstring)[0]
            mtime = int(self.__re['mtime'].findall(jsonstring)[0])
            rsize = int(self.__re['size'].findall(jsonstring)[0])
            rmd5 = self.__re['md5'].findall(jsonstring)[0]
        except Exception, e:
            sys.stderr.write('%s ERROR: Get pcs file meta failed: %s (Exception: %s).\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), jsonstring, e.message))
            return -1, '', 0, 0, ''
        return isdir, fullpath, mtime, rsize, rmd5

    def get_pcs_filelist(self, pcspath, startindex, endindex):
        # pcspath		: pcs full path
        # startindex	: start index number
        # endindex	: end index number
        uripath = quote_plus(pcspath)
        http_code, response_str = self.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=list&access_token=%s&path=%s&limit=%d-%d&by=name&order=asc' % (self.__syncytoken['access_token'], uripath, startindex, endindex), '', 'GET', 'normal')
        if http_code != 200:
            m = self.__re['error_code'].findall(response_str)
            if m and int(m[0]) == 31066:
                return 31066, []
            else:
                sys.stderr.write('%s ERROR: Get file list failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, pcspath, response_str))
                return 1, []
        m = self.__re['getlist'].findall(response_str)
        if m and len(m[0]) == 0:
            return 0, []
        elif m:
            response_str = m[0]
        else:
            sys.stderr.write('%s ERROR: Get file list failed(code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, pcspath, response_str))
            return 1, []
        response_str = response_str.replace('\/', '/').replace('"', '\\"')
        response_str = eval('u"' + response_str + '"').encode('utf8')
        response_str = self.__re['listrep'].sub('}\n{"fs_id', response_str)
        filelist = response_str.split('\n')
        return 0, filelist

class SYTrans(threading.Thread):
    def __init__(self, syconfig, sytoken, syncydb, argvs):
        threading.Thread.__init__(self)
        self.__config = syconfig
        self.__syncytoken = sytoken
        self.__syncydb = syncydb
        self.__op = argvs[0]
        self.__filepath = argvs[1]  # local file full path
        self.__fmtime = argvs[2]  # local file mtime
        self.__fsize = argvs[3]  # local file size
        self.__fnmd5 = argvs[4]  # md5 value of filepath string(binary)
        self.__pcspath = argvs[5]  # pcs full path
        self.__rsize = argvs[6]  # remote file size
        self.__rmd5 = argvs[7]  # remote file md5(hex)
        self.__ondup = argvs[8]  # ondup
        self.__syutil = SYUtil(self.__config)

    def run(self):
        ret = 1
        if self.__op == 'upload':
            if os.path.exists(self._filepath + '.tmp.syy'):
                ret = self.__slice_uploadfile()
            else:
                if self._fsize <= 262144:  # if file size less-than-or-equal-to  256K
                    ret = self.__upload_file()
                else:
                    ret = self.__rapid_uploadfile()
        elif self.__op == 'download':
            ret = self.__download_file()
        else:
            sys.stderr.write('%s Unknown parameters(%s) of threading operation.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__op))
        global SYNCCOUNT
        global ERRORCOUNT
        EXLock.acquire()
        if ret == 0:
            SYNCCOUNT += 1
        else:
            ERRORCOUNT += 1
        EXLock.release()
        TaskSemaphore.release()

    def __save_data(self):
        sydb = open(self.__syncydb, 'ab')
        try:
            fcntl.flock(sydb, fcntl.LOCK_EX)
            rmd5 = self.__rmd5.decode('hex')  # 16 bytes
            fmtime = struct.pack('>I', self.__fmtime)  # 4 bytes
            fsize = struct.pack('>I', self.__fsize % 4294967296)  # 4 bytes
            sydb.write(rmd5 + fmtime + fsize + self.__fnmd5)  # all length 40 bytes.
            sydb.flush()    # v2.0新增
            os.fsync(sydb)    # v2.0新增
            # fcntl.flock(sydb, fcntl.LOCK_UN)
        except os.error:
            sys.stderr.write('%s Error: save sync data failed.\n' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        finally:
            sydb.close()

    def __md5sum(self):
        with open(self.__filepath, "rb") as fh:
            m = hashlib.md5()
            fbuffer = fh.read(8192)
            while fbuffer:
                m.update(fbuffer)
                fbuffer = fh.read(8192)
            fh.close()
            cmd5 = m.hexdigest()
        return cmd5

    def __rapid_checkcode(self):
        with open(self.__filepath, "rb") as fh:
            m = hashlib.md5()
            fbuffer = fh.read(8192)
            crc = 0
            while fbuffer:
                m.update(fbuffer)
                # crc = binascii.crc32(fbuffer, crc) & 0xffffffff
                crc = zlib.crc32(fbuffer, crc) & 0xffffffff
                fbuffer = fh.read(8192)
            cmd5 = m.hexdigest()
            m = hashlib.md5()
            fh.seek(0)
            for i in range(32):
                fbuffer = fh.read(8192)
                m.update(fbuffer)
            fh.close()
        return '%x' % crc, cmd5, m.hexdigest()
        # print('0x%x' % (binascii.crc32('abc') & 0xffffffff))
        # print('%x' % (binascii.crc32('abc') & 0xffffffff))

    def __upload_file(self):
        uripath = quote_plus(self.__pcspath)
        http_code, response_str = self.__syutil.request('https://c.pcs.baidu.com/rest/2.0/pcs/file?method=upload&access_token=%s&path=%s&ondup=%s' % (self.__syncytoken['access_token'], uripath, self.__ondup), '', 'POST', 'upfile', self.__filepath)
        if http_code != 200:
            sys.stderr.write('%s ERROR: Upload file to pcs failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, self.__filepath, response_str))
            return 1
        m = self.__syutil.re['size'].findall(response_str)
        if m and int(m[0]) == self.__fsize:
            m = self.__syutil.re['md5'].findall(response_str)
            self.__rmd5 = m[0]
        else:
            sys.stderr.write('%s ERROR: Upload File failed, remote file error : %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath, response_str))
            self.__syutil.rm_pcsfile(self.__pcspath, 's')
            return 1
        self.__save_data()
        print('%s Upload file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
        return 0

    def __rapid_uploadfile(self):
        # rapid upload file by file md5
        crc, contentmd5, slicemd5 = self.__rapid_checkcode()
        uripath = quote_plus(self.__pcspath)
        # http_code,response_str = self._syutil.Request('https://pcs.baidu.com/rest/2.0/pcs/file?method=rapidupload&access_token=%s&path=%s&content-length=%d&content-md5=%s&slice-md5=%s&ondup=%s' % (self._syncytoken['access_token'], uripath, self._fsize, contentmd5, slicemd5, self._ondup),'','POST','normal')
        http_code, response_str = self.__syutil.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=rapidupload&access_token=%s&path=%s&content-length=%d&content-md5=%s&slice-md5=%s&content-crc32=%s&ondup=%s' % (self.__syncytoken['access_token'], uripath, self._fsize, contentmd5, slicemd5, crc, self.__ondup), '', 'POST', 'normal')
        if http_code != 200:
            m = self.__syutil.re['error_code'].findall(response_str)
            if m and int(m[0]) == 31079:
                print('%s File md5 not found, upload the whole file "%s".' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
                # print('%s httpcode:%s , md5:%s, CRC32: %s, %s.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, contentmd5, crc, response_str))
                # sys.exit(0)
                if self.__fsize <= self.__config['blocksize'] * 1048576 + 1048576:  # if file size less-than-or-equal-to  10M
                    return self.__upload_file()
                else:
                    return self.__slice_uploadfile()
            else:
                sys.stderr.write('%s ERROR: Rapid upload file failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, self.__filepath, response_str))
                return 1
        else:
            m = self.__syutil.re['size'].findall(response_str)
            if m and int(m[0]) == self.__fsize:
                m = self.__syutil.re['md5'].findall(response_str)
                self.__rmd5 = m[0]
            else:
                sys.stderr.write('%s ERROR: File is rapiduploaded,but can not get remote file size: %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath, response_str))
                return 1
            self.__save_data()
            print('%s Rapid upload file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
            return 0

    def __slice_uploadfile(self):
        #  Slice upload file, enables resuming,block size 10485760byte=10M
        if self.__fsize <= (self.__config['blocksize'] + 1) * 1048576:  # if file size less-than-or-equal-to  11M
            return self.__upload_file()
        elif self.__fsize > self.__config['blocksize'] * 1073741824:  # blocksize * 1M * 1024
            sys.stderr.write('%s ERROR: File "%s" size exceeds the setting, maxsize = blocksize * 1024M.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
            return 1
        startblk = 0
        upblkcount = self.__config['blocksize']
        param = 'param={"block_list":['
        if os.path.exists(self.__filepath + '.tmp.syy'):
            ulfn = open(self.__filepath + '.tmp.syy', 'r')
            upinfo = ulfn.readlines()
            ulfn.close()
            if upinfo[0].strip('\n') != 'upload %d %d' % (self.__fmtime, self.__fsize):
                ulfn = open(self.__filepath + '.tmp.syy', 'w')
                ulfn.write('upload %d %d\n' % (self.__fmtime, self.__fsize))
                ulfn.close()
                print('%s Local file:"%s" is modified, reupload the whole file.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
            else:
                for i in range(1, len(upinfo)):
                    blmd5, bllen = upinfo[i].strip('\n').split(' ')[1:]
                    if blmd5 == '':
                        continue
                    if startblk == 0:
                        param += '"' + blmd5 + '"'
                    else:
                        param += ',"' + blmd5 + '"'
                    startblk += int(bllen)
                print('%s Resuming slice upload file "%s".' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
        else:
            ulfn = open(self.__filepath + '.tmp.syy', 'w')
            ulfn.write('upload %d %d\n' % (self.__fmtime, self.__fsize))
            ulfn.close()
        while startblk * 1048576 < self.__fsize:
            if self.__fsize > (startblk + self.__config['blocksize'] + 1) * 1048576:
                upblocklen = upblkcount * 1048576
            else:
                upblocklen = self.__fsize - startblk * 1048576
                upblkcount = self.__config['blocksize'] + 1
            slicerange = str(startblk * 1048576) + ':' + str(upblocklen)  # offset:length
            http_code, response_str = self.__syutil.request('https://c.pcs.baidu.com/rest/2.0/pcs/file?method=upload&access_token=%s&type=tmpfile' % (self.__syncytoken['access_token']), slicerange, 'POST', 'upfile', self.__filepath)
            if http_code != 200:
                sys.stderr.write('%s ERROR: Slice upload file failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, self.__filepath, response_str))
                return 1
            blockmd5 = self.__syutil.re['md5'].findall(response_str)[0]
            ulfn = open(self.__filepath + ".tmp.syy", 'a')
            ulfn.write('md5-%d %s %d\n' % (startblk, blockmd5, upblkcount))
            ulfn.close()
            if startblk == 0:
                param += '"' + blockmd5 + '"'
            else:
                param += ',"' + blockmd5 + '"'
            startblk += upblkcount
        param += ']}'
        uripath = quote_plus(self.__pcspath)
        http_code, response_str = self.__syutil.request('https://pcs.baidu.com/rest/2.0/pcs/file?method=createsuperfile&access_token=%s&path=%s&ondup=%s' % (self.__syncytoken['access_token'], uripath, self.__ondup), param, 'POST', 'normal')
        if http_code != 200:
            sys.stderr.write('%s ERROR: Create superfile failed(error code:%d): %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, self.__filepath, response_str))
            return 1
        os.remove(self.__filepath + '.tmp.syy')
        m = self.__syutil.re['size'].findall(response_str)
        if m and int(m[0]) == self.__fsize:
            self.__rmd5 = self.__syutil.re['md5'].findall(response_str)[0]
        else:
            sys.stderr.write('%s ERROR: Slice upload file failed: %s, %s.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath, response_str))
            self.__syutil.rm_pcsfile(self.__pcspath, 's')
            return 1
        self.__save_data()
        print('%s Slice upload file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__filepath))
        return 0

    def __download_file(self):
        if os.path.exists(self.__filepath + '.tmp.syy'):
            dlfn = open(self.__filepath + '.tmp.syy', 'r')
            dlinfo = dlfn.readlines()
            dlfn.close()
            if dlinfo[0].strip('\n') != 'download %s %d' % (self.__rmd5, self.__rsize):
                dlfn = open(self.__filepath + '.tmp.syy', 'w')
                dlfn.write('download %s %d\n' % (self.__rmd5, self.__rsize))
                dlfn.close()
                print('%s Remote file:"%s" is modified, redownload the whole file.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__pcspath))
                os.remove(self.__filepath)
            else:
                print('%s Resuming download file "%s".' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__pcspath))
        else:
            dlfn = open(self.__filepath + '.tmp.syy', 'w')
            dlfn.write('download %s %d\n' % (self.__rmd5, self.__rsize))
            dlfn.close()
        uripath = quote_plus(self.__pcspath)
        if self.__config['slicedownload'] == 'off':
            http_code = self.__syutil.request('https://d.pcs.baidu.com/rest/2.0/pcs/file?method=download&access_token=%s&path=%s' % (self.__syncytoken['access_token'], uripath), '', 'GET', 'downfile', self.__filepath)
        else:
            http_code = self.__syutil.request_SD('https://d.pcs.baidu.com/rest/2.0/pcs/file?method=download&access_token=%s&path=%s' % (self.__syncytoken['access_token'], uripath), 'GET', self.__filepath, self.__rsize)
        if http_code != 200 and http_code != 206:  # 206断点下载时返回代码为 206 Partial Content
            sys.stderr.write('%s ERROR: Download file failed(error code:%d): "%s".\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), http_code, self.__pcspath))
            return 1
        fmeta = os.stat(self.__filepath)
        os.remove(self.__filepath + '.tmp.syy')
        if fmeta.st_size != self.__rsize:
            sys.stderr.write('%s ERROR: Download file failed: "%s", downloaded file size not equal to remote file size.\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__pcspath))
            os.remove(self.__filepath)
            return 1
        self.__fmtime = int(fmeta.st_mtime)
        self.__fsize = fmeta.st_size
        self.__save_data()
        print('%s Download file "%s" completed.' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), self.__pcspath))
        return 0

SYNCCOUNT = 0
ERRORCOUNT = 0
# ThreadCond = threading.Condition()
EXLock = threading.Lock()   # 修改全局变量SYNCCOUNT、ERRORCOUNT时需要加此锁
DBLock = threading.Lock()   # 写syncydb文件时需要加此锁

sy = SyncY(sys.argv[1:])
TaskSemaphore = threading.Semaphore(sy.tasknumber)    # 下载任务数信号量，创建下载任务线程时 P 操作，线程退出时 V 操作
ThreadSemaphore = threading.Semaphore(sy.threadnumber)    # 每个任务的线程数信号量，创建线程时 P 操作，线程退出时 V 操作
sy.start()
sys.exit(0)
