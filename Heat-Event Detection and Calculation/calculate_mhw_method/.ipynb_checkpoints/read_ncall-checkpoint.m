function varargout=read_ncall(varargin);

%
% Input variables:
%
%   xname lon;    xrange  [120 300]
%   yname lat;    yrange  [-30 30]
%   zname depth;  zrange  [0 50]
%   tname time;   trange  [datenum(2010,1,1) datenum(2017,1,1)]
%   tscale; units with day; tbase; datenum(1900,1,1)
%   vscale; change variable units 
%
% sample:
% [data,lon,lat,time]=read_ncall('fname',foname,'vname','RN_485','tbase',datenum(1997,4,16,12,0,0),'vs',24);
% [data,lon,lat,time]=read_ncall('fn',fmname,'vn','MLD_003','gnc','mom6','xr',lon_range,'yr',lat_range)
% [data,lon,lat,time]=read_ncall('fn',foname,'vn','sst','tb',datenum(1981,9,1),'xr',lon_range,'yr',lat_range)
tic
% lon
[found, filename, varargin] = parseparam(varargin, 'fname');
if ~found
    [found, filename, varargin] = parseparam(varargin, 'fn');
end
if ~found
    error('no filename input')
end
fstat=dir(filename);
%disp(['filename: ', filename,'; Size: ', num2str(fstat.bytes/1024/1024/1024), 'G'])
fprintf(['Filename: ', filename,'; Size: ', num2str(fstat.bytes/1024/1024/1024,'%.3f'), 'G; '])

% lon
[found, xname, varargin] = parseparam(varargin, 'xname');
if ~found
    [found, xname, varargin] = parseparam(varargin, 'xn');
    if ~found; 
        xname = 'lon'; 
        found=check_var_nc(filename,'LON'); if found; xname='LON'; end
        found=check_var_nc(filename,'longitude'); if found; xname='longitude'; end
        found=check_var_nc(filename,'LONGITUDE'); if found; xname='LONGITUDE'; end
        found=check_var_nc(filename,'Longitude'); if found; xname='Longitude'; end
        found=check_var_nc(filename,'X');         if found; xname='X'; end
        found=check_var_nc(filename,'xh');        if found; xname='xh'; end
        found=check_var_nc(filename,'xq');        if found; xname='xq'; end
    end
end
[xrange_found, xrange, varargin] = parseparam(varargin, 'xrange');
if ~xrange_found
    [xrange_found, xrange, varargin] = parseparam(varargin, 'xr');
end

% lat
[found, yname, varargin] = parseparam(varargin, 'yname');
if ~found
    [found, yname, varargin] = parseparam(varargin, 'yn');
    if ~found
        yname = 'lat';
        found=check_var_nc(filename,'LAT'); if found; yname='LAT'; end
        found=check_var_nc(filename,'latitude'); if found; yname='latitude'; end
        found=check_var_nc(filename,'LATITUDE'); if found; yname='LATITUDE'; end
        found=check_var_nc(filename,'Latitude'); if found; yname='Latitude'; end
        found=check_var_nc(filename,'Y');        if found; yname='T'; end
        found=check_var_nc(filename,'yh');       if found; yname='yh'; end
        found=check_var_nc(filename,'yq');       if found; yname='yq'; end
    end
end
[yrange_found, yrange, varargin] = parseparam(varargin, 'yrange');
if ~yrange_found
    [yrange_found, yrange, varargin] = parseparam(varargin, 'yr');
end

% time
[found, tname, varargin] = parseparam(varargin, 'tname');
if ~found
    [found, tname, varargin] = parseparam(varargin, 'tn');
    if ~found
        tname = 'time';
        found=check_var_nc(filename,'TIME'); if found; tname='TIME'; end
        found=check_var_nc(filename,'Time'); if found; tname='Time'; end
        found=check_var_nc(filename,'T');    if found; tname='T'; end
    end
end
[trange_found, trange, varargin] = parseparam(varargin, 'trange');
if ~trange_found
    [trange_found, trange, varargin] = parseparam(varargin, 'tr');
end
if trange_found
   if trange(2)<=trange(1); error('date range is wrong!'); end
end

[found, tscale, varargin] = parseparam(varargin, 'tscale');
if ~found
    [found, tscale, varargin] = parseparam(varargin, 'ts');
    if ~found
        tscale =1;
    end
end
[tbase_found, tbase, varargin] = parseparam(varargin, 'tbase');
if ~tbase_found
    [tbase_found, tbase, varargin] = parseparam(varargin, 'tb');
    if ~tbase_found
        tbase =datenum(1900,1,1);
    end
end

%mom files
[found, gnc, varargin] = parseparam(varargin, 'gnc'); %gnc: nc group, mom6 result nc belong to one group, same for others
if found & strcmp(gnc,'mom6')
    xname = 'xh'; yname = 'yh'; tname = 'time'; zname = 'z_l';
elseif found & strcmp(gnc,'normal')
    xname = 'lon'; yname = 'lat'; tname = 'time'; zname = 'z_l';
end

% z
[zname_found, zname, varargin] = parseparam(varargin, 'zname');
if ~zname_found
    [zname_found, zname, varargin] = parseparam(varargin, 'zn');
    if ~zname_found
        zname = 'z';
    end
end
[zrange_found, zrange, varargin] = parseparam(varargin, 'zrange');
if ~zrange_found
    [zrange_found, zrange, varargin] = parseparam(varargin, 'zr');
end

% circ shift lon
[circshift_x_found, circshift_x, varargin] = parseparam(varargin, 'circshift_x');
if ~circshift_x_found
    [circshift_x_found, circshift_x, varargin] = parseparam(varargin, 'cx');
end
[reverse_y_found, reverse_y, varargin] = parseparam(varargin, 'reverse_y');
if ~reverse_y_found
    [reverse_y_found, reverse_y, varargin] = parseparam(varargin, 'ry');
end
[permute_found, permute_sign, varargin] = parseparam(varargin, 'permute');
if ~permute_found
    [permute_found, permute_sign, varargin] = parseparam(varargin, 'pm');
end
if ~permute_found
    permute_sign='on';
end

% variable scale, to change units
[found, vname, varargin] = parseparam(varargin, 'vname');
if ~found
    [found, vname, varargin] = parseparam(varargin, 'vn');
end

[vnanp_found, vnanp, varargin] = parseparam(varargin, 'vnanp');
[vnann_found, vnann, varargin] = parseparam(varargin, 'vnann');
    
[found, vscale, varargin] = parseparam(varargin, 'vscale');
if ~found
    [found, vscale, varargin] = parseparam(varargin, 'vs');
    if ~found 
        vscale =1;
    end
end

% read lon and found i number along x
lon0=double(ncread(filename,xname));

% if xrange is out of range, then will read all the data along x
if xrange_found 
   if (min(lon0)>xrange(1) | max(lon0)<xrange(2))
       xrange_found='false';
   end
end

lon0(lon0<0)=lon0(lon0<0)+360;
lon0(lon0>360)=lon0(lon0>360)-360;
londif=nanmean(diff(lon0));
if (lon0(end)-lon0(1))>359 & (lon0(1)-0>londif | 360-lon0(end)>londif) & ~circshift_x_found
   error(['lon start from ',num2str(lon0(1)),'-',num2str(lon0(end)),', should circ shift lon'])
end

if xrange_found & ~circshift_x_found
   [indi]=find(lon0>=xrange(1) & lon0<=xrange(2));
   ib=min(indi);ie=max(indi);in=ie-ib+1;
   if ib~=indi(1) & ie~=indi(end) & (min(lon0)>xrange(1) | max(lon0)<xrange(2))
      error(['lon start from ',num2str(lon0(1)),'-',num2str(lon0(end)),', should adjust x range or circ shift lon'])
   end
   if length(indi)==0
      error(['can not find value in this xrange ',num2str(xrange(1)),'-',num2str(xrange(end)),', should adjust x range '])
   end
else
   ib=1;ie=length(lon0);
   [im0,jm0]=size(lon0);
   if jm0>1 & im0>1; ie=im0; end
   in=ie-ib+1;
end

% read lat and found j number along x
lat0=double(ncread(filename,yname));
latdif=nanmean(diff(lat0));
if lat0(1)>lat0(end) & ~reverse_y_found
   error(['lat start from ',num2str(lat0(1)),'-',num2str(lat0(end)),', should reverse lat'])
end

% if yrange is out of range, then will read all the data along y
if yrange_found
   if (min(lat0)>yrange(1) | max(lat0)<yrange(2))
      yrange_found='false';
   end
end

if yrange_found
   [indj]=find(lat0>=yrange(1) & lat0<=yrange(2));
   jb=min(indj);je=max(indj);jn=je-jb+1;
   if jb~=indj(1) & je~=indj(end) & (min(lat0)>yrange(1) | max(lat0)<yrange(2))
      error(['lat start from ',num2str(lat0(1)),'-',num2str(lat0(end)),', should adjust y range'])
   end
   if length(indj)==0
      error(['can not find value in this yrange ',num2str(yrange(1)),'-',num2str(yrange(end)),', should adjust y range '])
   end
else
   jb=1;je=length(lat0);
   [im0,jm0]=size(lat0);
   if jm0>1 & im0>1; je=jm0; end
   jn=je-jb+1;
end

% read dep and found k number along z
if zname_found
   dep0=double(ncread(filename,zname));
   if zrange_found & max(dep0)*max(zrange)<0
      error(['depth start from ',num2str(dep0(1)),'-',num2str(dep0(end)),', should redefine z range'])
   end
else
   dep0=nan;
end
if zrange_found
   [indk]=find(dep0>=zrange(1) & dep0<=zrange(2));
   kb=min(indk);ke=max(indk);kn=ke-kb+1;
else
   kb=1;ke=length(dep0);kn=ke-kb+1;
end

vinfo = ncinfo(filename,vname);
vsize=length(vinfo.Size);

[dimension_order_found, dimension_order, varargin] = parseparam(varargin, 'dimension_order');
if ~dimension_order_found
    [dimension_order_found, dimension_order, varargin] = parseparam(varargin, 'do');
    if ~dimension_order_found
        if vsize==2;     dimension_order = ['ij'];
        elseif vsize==3 & strcmp(vinfo.Dimensions(3).Name,tname); dimension_order = ['ijl'];
        %elseif vsize==3 & strcmp(vinfo.Dimensions(3).Name,zname); dimension_order = ['ijk'];
        elseif vsize==3; dimension_order = ['ijk'];
        elseif vsize==4; dimension_order = ['ijkl'];
        else;            error('dimension is wrong');
        end
    end
end

idiml=strfind(dimension_order,'l');
idimk=strfind(dimension_order,'k');
if length(idiml)>=1
   [time_exist,time_att_exist]=check_var_nc(filename,tname,'units');
   if time_att_exist;
      time_units=ncreadatt(filename,tname,'units');
      time_units=strsplit(time_units);
      if strcmpi(char(time_units(1)),'days'); tscale=1; end
      if strcmpi(char(time_units(1)),'hours'); tscale=1/24; end
      if strcmpi(char(time_units(1)),'seconds'); tscale=1/24/3600; end
      if strcmpi(char(time_units(1)),'months'); tscale=1*30.5; end
      %if ~strcmpi(char(time_units(1)),'days') & tscale==1
      if ~contains(char(time_units(1)),'day','IgnoreCase',true) & tscale==1
         error('time units is not days and a scale should be provided');
      end

      if length(time_units)>=3; tbase=datenum(char(time_units(3)),'yyyy-mm-dd'); tbase_found=true; end
      if length(time_units)>=3 & datenum(char(time_units(3)),'yyyy-mm-dd')~=693962 & ~tbase_found
         error('time base is not 1900-01-01 and a new time base should be provided');
      end
   end

   time0=double(ncread(filename,tname))*tscale;
   if time_att_exist 
      if strcmpi(char(time_units(1)),'months'); 
	 time0=datenum(0,1:length(time0),15); 
   end; end
   %time0=double(ncread(filename,tname))*tscale+tbase;
   [time_exist,time_att_exist]=check_var_nc(filename,tname,'calendar');
   if time_att_exist;
      time_calendar=ncreadatt(filename,tname,'calendar');
      if contains(time_calendar,'365','IgnoreCase',true) | contains(time_calendar,'noleap','IgnoreCase',true);
         disp(['calendar is 365_day and switching to gregorian']);
         time0=daynoleap2datenum(time0, 0);
      end
   end 
   time0=time0+tbase;
else
   time0=nan;
end

if trange_found
   [indl]=find(time0>=trange(1) & time0<=trange(2));
   lb=min(indl);le=max(indl);ln=le-lb+1;
else
   lb=1;le=length(time0);ln=le-lb+1;
end

bstart=nan(1,vsize);bcount=nan(1,vsize);porder=nan(1,vsize);
if vsize>=2;
   ind=strfind(dimension_order,'i'); bstart(ind)=ib; bcount(ind)=in; porder(2)=ind;
   ind=strfind(dimension_order,'j'); bstart(ind)=jb; bcount(ind)=jn; porder(1)=ind;
end
n=2;
if vsize>=3 & length(idimk)>=1
   n=n+1;
   ind=strfind(dimension_order,'k'); bstart(ind)=kb; bcount(ind)=kn; porder(n)=ind;
end
if vsize>=3 & length(idiml)>=1;
   n=n+1;
   ind=strfind(dimension_order,'l'); bstart(ind)=lb; bcount(ind)=ln; porder(n)=ind;
end

if vsize==2;
   data=double(ncread(filename,vname,bstart,bcount));
   if strcmp(permute_sign,'on')
      data=data';
   end
   data=data*vscale;
   timeout=nan;
elseif vsize==3 & length(idimk)>=1;
   data=double(ncread(filename,vname,bstart,bcount));
   if strcmp(permute_sign,'on')
      data=permute(data,porder);
   end
   data=data*vscale; %unit adjustment
   timeout=time0(lb:le);
   depout=dep0(kb:ke);
elseif vsize==3 & length(idiml)>=1;
   data=double(ncread(filename,vname,bstart,bcount));
   if strcmp(permute_sign,'on')
      data=permute(data,porder);
   end
   data=data*vscale; %unit adjustment
   timeout=time0(lb:le);
elseif vsize==4;
   data=double(ncread(filename,vname,bstart,bcount));
   if strcmp(permute_sign,'on')
      data=permute(data,porder);
   end
   data=data*vscale; %unit adjustment
   timeout=time0(lb:le);
   depout=dep0(kb:ke);
else
   error('variable dimension is wrong!')
end

if vnanp_found
   data(data>=vnanp)=nan;
end

if vnann_found
   data(data<=vnann)=nan;
end

[lon,lat]=meshgrid(lon0(ib:ie),lat0(jb:je));
if circshift_x_found
   lon=circshift(lon,circshift_x,2);
   lat=circshift(lat,circshift_x,2);
   data=circshift(data,circshift_x,2);
   if xrange_found
      [indj,indi]=find(lon>=xrange(1) & lon<=xrange(2) & lat>=yrange(1) & lat<=yrange(2));
      jb=min(indj);je=max(indj);ib=min(indi);ie=max(indi);
      lon=lon(jb:je,ib:ie);
      lat=lat(jb:je,ib:ie);
      data=data(jb:je,ib:ie,:,:);
   end
end



if reverse_y_found
   lon=flip(lon,1);
   lat=flip(lat,1);
   data=flip(data,1);
end


if nargout >= 1
    varargout{1} = data;
end

if nargout >= 3
    varargout{2} = lon;
    varargout{3} = lat;
end

if nargout >= 4 & vsize==3 & strcmp(vinfo.Dimensions(3).Name,zname);
    varargout{4} = depout;
elseif nargout >= 4
    varargout{4} = timeout;
end

if nargout >= 5
    varargout{5} = depout;
end

etime=toc;
fprintf([' loading time is ',num2str(etime,'%.2f'),' seconds\n']);
return

