package com.wugui.datax.admin.core.scheduler;

import com.wugui.datatx.core.biz.ExecutorBiz;
import com.wugui.datatx.core.enums.ExecutorBlockStrategyEnum;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.thread.*;
import com.wugui.datax.admin.core.util.I18nUtil;
import com.wugui.datax.rpc.remoting.invoker.call.CallType;
import com.wugui.datax.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.wugui.datax.rpc.remoting.invoker.route.LoadBalance;
import com.wugui.datax.rpc.remoting.net.impl.netty_http.client.NettyHttpClient;
import com.wugui.datax.rpc.serialize.impl.HessianSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);


    public void init() throws Exception {
        // init i18n
        // 1 初始化页面国际化的工具
        initI18n();

        // admin registry monitor run
        // 2 启动注册监控线程
        JobRegistryMonitorHelper.getInstance().start();

        // admin monitor run
        // 3 启动失败监控线程
        JobFailMonitorHelper.getInstance().start();

        // admin trigger pool start
        // 4 初始化触发线程池，创建快慢线程池
        JobTriggerPoolHelper.toStart();

        // admin log report start
        // 5 启动日志线程
        JobLogReportHelper.getInstance().start();

        // start-schedule
        // 6 启动作业调度器，这个类是主要逻辑.启动一个死循环，不断的遍历任务触发执行。为避免cpu飙升，隔一会睡一会(这里是重点)
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init datax-web admin success.");
    }


    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

        // admin monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryMonitorHelper.getInstance().toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address == null || address.trim().length() == 0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        XxlRpcReferenceBean referenceBean = new XxlRpcReferenceBean();
        referenceBean.setClient(NettyHttpClient.class);
        referenceBean.setSerializer(HessianSerializer.class);
        referenceBean.setCallType(CallType.SYNC);
        referenceBean.setLoadBalance(LoadBalance.ROUND);
        referenceBean.setIface(ExecutorBiz.class);
        referenceBean.setVersion(null);
        referenceBean.setTimeout(3000);
        referenceBean.setAddress(address);
        referenceBean.setAccessToken(JobAdminConfig.getAdminConfig().getAccessToken());
        referenceBean.setInvokeCallback(null);
        referenceBean.setInvokerFactory(null);

        executorBiz = (ExecutorBiz) referenceBean.getObject();

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
