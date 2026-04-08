clear;close all

%calculate mclim,m90
fin_clim = 'era5_temperature_1985_2014.nc';
fout_clim = 'climc90_1985_2014.mat';
fout_mhw='MHW_2015_2024.mat';
tb_clim=datenum(1985,1,1); te_clim=datenum(2014,12,31);
[sst,lon,lat,time]=read_ncall('fn',fin_clim,'vn','t2m','tb',datenum(1,1,1));

[mclim,m90]=mhw_clim(sst,time,tb_clim,te_clim);
save(fout_clim,'mclim','m90');

%calculate MHW
var= char('t2m');
fin='era5_t2m_2015_2024.nc';
fout_daily=[fin(1:end-3),'_mhw_daily.nc'];
[sst,lon,lat,time]=read_ncall('fn',fin,'vn','t2m','tb',datenum(1,1,1));
tb_mhw=time(1); te_mhw=time(end);
load(fout_clim);
tic;disp(['detecting the marine heat waves']);
[MHW,mclim,m90,mhw_ts]=mhw_detect(sst,time,m90,mclim,tb_mhw,te_mhw);
save(fout_mhw,'MHW');
%stop

%toc;disp(['detecting the marine heat waves']);
%metrics_all=mhw_metrics(MHW,mhw_ts,time); toc
var_all={[var,'_mhw'],'^oC','marine heat wave daily intensity'};
delete(fout_daily)
ncall_create('fn',fout_daily,'vn',var_all,'gnc','3d','xd',lon(1,:),'yd',lat(:,1),'vd',permute(mhw_ts,[2,1,3]),'td',time-datenum(1900,1,1),'tb',datenum(1900,1,1));
   