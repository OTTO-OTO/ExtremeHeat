function varargout=create_ncall(varargin);

%
% 2d nc: vnall={'Idamp','s-1','damp rate for tracer T and S'}; data=Idamp';
% varargout=create_ncall('fn',f_out,'vn',vnall,'xn','xh','yn','yh','xd',xh,'yd',yh,'vd',data);
% ncout=create_ncall('fn',fout_name,'vn',var_temp,'xn','xh','yn','yh','tn','time','zn','z_l','tbndn','time_bnds','xd',xh,'yd',yh,'zd',z_l,'td',time_all,'tbndd',time_bnds_all','vd',permute(temp_all,[2,1,3,4]));
%
tic
% filename
[found_fn, filename, varargin] = parseparam(varargin, 'fname');
if ~found_fn
    [found_fn, filename, varargin] = parseparam(varargin, 'fn');
    if ~found_fn
       error('no filename input')
    end
end

% lon
[found_xn, xn, varargin] = parseparam(varargin, 'xname');
if ~found_xn
    [found_xn, xn, varargin] = parseparam(varargin, 'xn');
    if ~found_xn;
       %disp(['no x coordinate (lon) is defined']); %xname = 'lon';
    else
       [found_xd, xd, varargin] = parseparam(varargin, 'xd'); %data on x coord, lon value
    end
end

% lat
[found_yn, yn, varargin] = parseparam(varargin, 'yname');
if ~found_yn
    [found_yn, yn, varargin] = parseparam(varargin, 'yn');
    if ~found_yn
       %disp(['no y coordinate (lat) is defined']); %xname = 'lat';
    else
       [found_yd, yd, varargin] = parseparam(varargin, 'yd'); %data on y coord, lat value
    end
end

% z
[found_zn, zn, varargin] = parseparam(varargin, 'zname');
if ~found_zn
    [found_zn, zn, varargin] = parseparam(varargin, 'zn');
    if ~found_zn
       %disp(['no z coordinate (dep) is defined']); %zname = 'z';
    else
       [found_zd, zd, varargin] = parseparam(varargin, 'zd'); %data on y coord, lat value
    end
end

% time
[found_tn, tn, varargin] = parseparam(varargin, 'tname');
found_td=false;
if ~found_tn
    [found_tn, tn, varargin] = parseparam(varargin, 'tn');
    if ~found_tn
       %disp(['no t coordinate (time) is defined']); %tname = 'time';
    else
       [found_td, td, varargin] = parseparam(varargin, 'td'); %data on y coord, lat value
    end
end

%mom files
[found, gnc, varargin] = parseparam(varargin, 'gnc'); %gnc: nc group, mom6 result nc belong to one group, same for others
if found & strcmp(gnc,'1d')
    tn = 'time';
    found_xn=false; found_yn=false; found_tn=true; found_zn=false;
    [found_td, td, varargin] = parseparam(varargin, 'td');
elseif found & strcmp(gnc,'2d')
    xn = 'lon'; yn = 'lat'; 
    found_xn=true; found_yn=true; found_tn=false; found_zn=false;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
elseif found & strcmp(gnc,'2dmom6')
    xn = 'xh'; yn = 'yh';
    found_xn=true; found_yn=true; found_tn=false; found_zn=false;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
elseif found & strcmp(gnc,'3d')
    xn = 'lon'; yn = 'lat'; tn='time'; 
    found_xn=true; found_yn=true; found_tn=true; found_zn=false;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
    [found_td, td, varargin] = parseparam(varargin, 'td');
elseif found & strcmp(gnc,'3dmom6')
    xn = 'xh'; yn = 'yh'; tn='time';
    found_xn=true; found_yn=true; found_tn=true; found_zn=false;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
    [found_td, td, varargin] = parseparam(varargin, 'td');
elseif found & strcmp(gnc,'3dnontime')
    xn = 'lon'; yn = 'lat'; zn='depth';
    found_xn=true; found_yn=true; found_tn=false; found_zn=true;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
    [found_zd, zd, varargin] = parseparam(varargin, 'zd');
elseif found & strcmp(gnc,'4d')
    xn = 'lon'; yn = 'lat'; tn='time'; zn='depth';
    found_xn=true; found_yn=true; found_tn=true; found_zn=true;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
    [found_zd, zd, varargin] = parseparam(varargin, 'zd');
    [found_td, td, varargin] = parseparam(varargin, 'td');
elseif found & strcmp(gnc,'4dmom6')
    xn = 'xh'; yn = 'yh'; tn='time'; zn='z_l';
    found_xn=true; found_yn=true; found_tn=true; found_zn=true;
    [found_xd, xd, varargin] = parseparam(varargin, 'xd');
    [found_yd, yd, varargin] = parseparam(varargin, 'yd');
    [found_zd, zd, varargin] = parseparam(varargin, 'zd');
    [found_td, td, varargin] = parseparam(varargin, 'td');
end
if found_xn; disp([' x coordinate (lon) is defined']); end
if found_yn; disp([' y coordinate (lat) is defined']); end
if found_zn; disp([' z coordinate (depth) is defined']); end
if found_tn; disp([' t coordinate (time) is defined']); end

[found_tb, tb, varargin] = parseparam(varargin, 'tbase');
if ~found_tb
    [found_tb, tb, varargin] = parseparam(varargin, 'tb');
    if ~found_tb
        tb =datenum(1900,1,1);
    end
end
if found_tn
   disp(['time based on ',num2str(datevec(tb))]);
end

[found_tm, tm, varargin] = parseparam(varargin, 'tmodule');
if ~found_tm
    [found_tm, tm, varargin] = parseparam(varargin, 'tm');
    if found_tn & ~found_tm
        disp(['time module is not written']); %xname = 'lat';
    end
end

[found_ta, ta, varargin] = parseparam(varargin, 'time_append');
if ~found_ta
    [found_ta, ta, varargin] = parseparam(varargin, 'tappend');
end
if found_tn & found_ta
    disp(['time appending is written']); 
end

[found_cn, cn, varargin] = parseparam(varargin, 'calendar_name');
if ~found_cn
    [found_cn, cn, varargin] = parseparam(varargin, 'cn');
    if ~found_cn
        cn ='gregorian';
    end
end
if found_tn
   disp(['Time calendar: ',cn]); %xname = 'lat';
end
[found_tbndn, tbndn, varargin] = parseparam(varargin, 'time_bnds_name');
if ~found_tbndn
    [found_tbndn, tbndn, varargin] = parseparam(varargin, 'tbndn');
    if found_tbndn
       [found_tbndd, tbndd, varargin] = parseparam(varargin, 'tbndd'); %data on time_bnds
    end
end

[found_zbndn, zbndn, varargin] = parseparam(varargin, 'z_bnds_name');
if ~found_zbndn
    [found_zbndn, zbndn, varargin] = parseparam(varargin, 'zbndn');
    if found_zbndn
       [found_zbndd, zbndd, varargin] = parseparam(varargin, 'zbndd'); %data on time_bnds
    end
end

% variable scale, to change units
[found_vn, vname_all, varargin] = parseparam(varargin, 'vname');
if ~found_vn
    [found_vn, vname_all, varargin] = parseparam(varargin, 'vn');
    if ~found_vn
       error('no variable is provided')
    else
       [found_vd, vd, varargin] = parseparam(varargin, 'vd'); %data on y coord, lat value
    end
end

[found_fv, fv, varargin] = parseparam(varargin, 'fillvalue');
if ~found_fv
    [found_fv, fv, varargin] = parseparam(varargin, 'fv');
    if ~found_fv
       fv=NaN; disp(['filling value is NaN']);
    end
end


vn=char(vname_all(1));
vn_unit=char(vname_all(2));
vn_longname=char(vname_all(3));
file_exist=isfile(filename);
if found_xn
   if file_exist; 
      var_exist_xn=check_var_nc(filename,xn);
   else
      var_exist_xn=file_exist;
   end
   if ~var_exist_xn
      im=length(xd);
      nccreate(filename,xn,'Dimensions',  {xn, im});
      ncwriteatt(filename, xn, 'standard_name', 'longitude');
      ncwriteatt(filename, xn, 'long_name', 'longitude');
      ncwriteatt(filename, xn, 'units', 'degrees_east');
      %ncwriteatt(filename, xn, 'point_spacing', 'even');
      ncwriteatt(filename, xn, 'axis', 'X');
   else
      xd=ncread(filename,xn);im=length(xd);
   end
end

%no need to check file below. If not exist, it should be created above
if found_yn
   var_exist_yn=check_var_nc(filename,yn);
   if ~var_exist_yn
      jm=length(yd);
      nccreate(filename,yn,'Dimensions',  {yn, jm});
      ncwriteatt(filename, yn, 'standard_name', 'latitude');
      ncwriteatt(filename, yn, 'long_name', 'latitude');
      ncwriteatt(filename, yn, 'units', 'degrees_north');
      ncwriteatt(filename, yn, 'axis', 'Y');
      %ncwriteatt(filename, yn, 'point_spacing', 'uneven');
   else
      yd=ncread(filename,yn);jm=length(yd);
   end
end

if found_zn
   var_exist_zn=check_var_nc(filename,zn);
   if ~var_exist_zn
      km=length(zd);
      nccreate(filename,zn,'Dimensions',  {zn, km});
      ncwriteatt(filename, zn, 'standard_name', 'Depth');
      ncwriteatt(filename, zn, 'long_name', 'Depth of center point in each layer');
      ncwriteatt(filename, zn, 'units', 'meters');
      ncwriteatt(filename, zn, 'axis', 'Z');
      ncwriteatt(filename, zn, 'cartesian_axis', 'Z');
      ncwriteatt(filename, zn, 'positive', 'down');
      if found_zbndn; ncwriteatt(filename, zn, 'bounds',zbndn);end
      %ncwriteatt(filename, zn, 'edges', 'z_i');
   else
      zd=ncread(filename,zn);km=length(zd);
   end
end

if found_zbndn
   var_exist_zbndn=check_var_nc(filename,zbndn);
   if ~var_exist_zbndn
      nccreate(filename,zbndn,'Dimensions',{'bnds', 2, zn, km});
   else
     zbndd=ncread(filename,zbndn);
   end
end

if found_tn
   if file_exist;
      var_exist_tn=check_var_nc(filename,tn);
   else
      var_exist_tn=file_exist;
   end
   %var_exist_tn=check_var_nc(filename,tn);

   if ~var_exist_tn
      lm=length(td);
      nccreate(filename,tn,'Dimensions',{tn, inf});
      ncwriteatt(filename, tn, 'standard_name', tn);
      ncwriteatt(filename, tn, 'long_name', tn);
      if found_tbndn; ncwriteatt(filename, tn, 'bounds',tbndn);end
      [yr,mo,da,hh,mm,ss]=datevec(tb);
      tbd=[num2str(yr),'-',num2str(mo,'%02.f'),'-',num2str(da,'%02.f'),' ',num2str(hh,'%02.f'),':',num2str(mm,'%02.f'),':',num2str(ss,'%02.f')];
      ncwriteatt(filename, tn, 'units', ['days since ',tbd]);
      ncwriteatt(filename, tn, 'calendar', cn); %calendar name
      ncwriteatt(filename, tn, 'axis', 'T');
      if found_tm; ncwriteatt(filename, tn, 'modulo', ' '); end; %
   else
      %add the appending time to the existing one
      td0=ncread(filename,tn);lm0=length(td);
      if found_ta & td0(end)>td(1);
	 error('time input is smaller than time existing');
      end
   end
end

if found_tbndn
   var_exist_tbndn=check_var_nc(filename,tbndn);
   if ~var_exist_tbndn
      nccreate(filename,tbndn,'Dimensions',{'bnds', 2, tn, inf});
   else
     tbndd=ncread(filename,tbndn);
     [lm1,lm2]=size(tbndd);
     if lm0~=lm1; error('check the code related to tn and time_bnds'); end
   end
end


if found_vn;
   var_exist_vn=check_var_nc(filename,vn);
   if var_exist_vn
      error('variable is already defined in the nc')
   end
end

%check variable byte size
var_byte_size=whos('vd');
var_byte_size=var_byte_size.bytes/1e9; %byte-->GB
if var_byte_size>2; %>2GB, in float 
   var_type='single'; %single
else
   var_type='single'; %var_type='double';
end

% 1d time
if found_tn & ~found_xn & ~found_yn & ~found_zn
   nccreate(filename,vn,'Dimensions',{tn,inf},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 1d lon
if found_xn & ~found_yn & ~found_zn & ~found_tn
   nccreate(filename,vn,'Dimensions',{xn,im},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 1d lat
if found_yn & ~found_xn & ~found_zn & ~found_tn
   nccreate(filename,vn,'Dimensions',{yn,jm},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 1d z
if found_zn & ~found_xn & ~found_yn & ~found_tn
   nccreate(filename,vn,'Dimensions',{zn,km},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 2d lon, lat
if found_xn & found_yn & ~found_zn & ~found_tn 
   nccreate(filename,vn,'Dimensions',{xn,im,yn,jm},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 2d lon, time
if found_xn & found_tn & ~found_yn & ~found_zn
   nccreate(filename,vn,'Dimensions',{xn,im,tn,inf},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 2d lat, time
if found_yn & found_tn & ~found_xn & ~found_zn
   nccreate(filename,vn,'Dimensions',{yn,jm,tn,inf},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 3d lon, lat, time
if found_xn & found_yn & found_tn & ~found_zn
   nccreate(filename,vn,'Dimensions',{xn,im,yn,jm,tn,inf},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
   %disp(['filling value is ', num2str(fv)]);
end

% 3d lon, lat, z
if found_xn & found_yn & found_zn & ~found_tn
   nccreate(filename,vn,'Dimensions',{xn,im,yn,jm,zn,km},'FillValue',fv,'Datatype',var_type);
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

% 4d lon, lat, z, time
if found_xn & found_yn & found_zn & found_tn
   %nccreate(filename,vn,'Dimensions',{xn,im,yn,jm,zn,km,tn,inf},'FillValue',NaN);
   nccreate(filename,vn,'Dimensions',{xn,im,yn,jm,zn,km,tn,inf},'FillValue',fv,'Datatype',var_type);
   %nccreate(filename,vn,'Dimensions',{xn,im,yn,jm,zn,km,tn,inf});
   ncwriteatt(filename, vn, 'long_name', vn_longname);
   ncwriteatt(filename, vn, 'units', vn_unit);
end

if found_td;
   if td(1)>=datenum(1900,1,1) & tb == datenum(1900,1,1);; td=td-datenum(1900,1,1); end
end

if found_xn & ~var_exist_xn; ncwrite(filename,xn,xd); end
if found_yn & ~var_exist_yn; ncwrite(filename,yn,yd); end
if found_zn & ~var_exist_zn; ncwrite(filename,zn,zd); end
if found_tn & ~var_exist_tn; ncwrite(filename,tn,td); end
if found_vn & ~var_exist_vn; ncwrite(filename,vn,vd); end
if found_tbndn & ~var_exist_tbndn; ncwrite(filename,tbndn,tbndd); end
if found_zbndn & ~var_exist_zbndn; ncwrite(filename,zbndn,zbndd); end

if found_tn & var_exist_tn & found_ta; ncwrite(filename,tn,td,lm0,1); end
if found_tbndn & var_exist_tbndn & found_ta; ncwrite(filename,tbndn,tbndd,[1,lm0],1); end
if found_vn & var_exist_vn & found_ta; 
   sz_all=length(size(vd));
   if sz_all==1; 
      ncwrite(fout,varname,data_deps,[lm0],[1]);
   elseif sz_all==2
      ncwrite(fout,varname,data_deps,[1,lm0],[1,1]);
   elseif sz_all==3
      ncwrite(fout,varname,data_deps,[1,1,lm0],[1,1,1]);
   elseif sz_all==4
      ncwrite(fout,varname,data_deps,[1,1,1,lm0],[1,1,1,1]);
   else
      error('data dimension is wrong!');
   end
end

%ncwriteatt(filename,'/','data_creator','Enhui Liao from Laure Resplandy Group at Princeton University (enhuil@princeton.edu)');
%ncwriteatt(filename,'/','data_creator','Enhui Liao from Laure Resplandy Group at Princeton University');
ncwriteatt(filename,'/','data_creator','Enhui Liao at School of Oceanography in Shanghai Jiao Tong University');
ncwriteatt(filename,'/','creation_date',datestr(now));
ncwriteatt(filename,'/','creation_location',pwd);

if nargout >= 1
    varargout{1} = true(1);
end

toc;


