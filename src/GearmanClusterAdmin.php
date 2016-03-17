<?php
namespace GCWorld\GearmanAdmin;

use Exception;

/**
 * Monitoring Gearman over telnet port 4730
 *
 * So the only way to monitor Gearman is via doing a telnet to port 4730. The
 * current monitoring supported commands are fairly basic.
 * There are plans to include more set of commands in the next release.
 * @version 1.1 (Modified by Lior Ben-Kereth)
 * A class that contains separated and aggragated workers and status data from all gearman server in a cluster
 *
 * @source https://github.com/preillyme/php
 * @author liorbk
 * @author preilly@php.net
 * @category   PHP
 * @package    GearmanTelnet
 * @copyright  Copyright (c) 2013 Patrick Reilly (http://www.facebook.com/preilly)
 */
class GearmanClusterAdmin
{
    private $accumaltiveJobs    = array();
    private $accumaltiveWorkers = array();

    private $serversJobs    = array();
    private $serversWorkers = array();

    private $hosts;
    private $orderFunction;

    /**
     *
     * @param array    $hosts         - array of host:port strings
     * @param \closure $orderFunction - a function that gets serversJob array return a manipulated array (for example, change job names, sort, etc)
     */
    public function __construct(array $hosts, $orderFunction = null)
    {
        $this->hosts         = $hosts;
        $this->orderFunction = $orderFunction;

        $this->init();
    }

    public function getAccumaltiveJobs()
    {
        return $this->accumaltiveJobs;
    }

    public function getAccumaltiveWorkers()
    {
        return $this->accumaltiveWorkers;
    }

    public function getServersJobs()
    {
        return $this->serversJobs;
    }

    public function getServersWorkers()
    {
        return $this->serversWorkers;
    }

    private function init()
    {
        // Run on all gearman servers and collect data and accumalate it
        foreach ($this->hosts as $_server) {
            try{
                $gm = new GearmanHost($_server);
            } catch(Exception $ex){
                continue;
            }
            $serverWorkers = $gm->getWorkers();
            $serverJobs    = $gm->getJobs();

            if (!empty($this->orderFunction) && is_callable($this->orderFunction)) {
                $serverJobs = call_user_func($this->orderFunction, $serverJobs);
            }

            $this->serversJobs[$_server]    = $serverJobs;
            $this->serversWorkers[$_server] = $serverWorkers;

            foreach ($serverJobs as $jobName => $job) {
                $total     = $job[GearmanHost::WORKER_TOTAL];
                $running   = $job[GearmanHost::WORKER_RUNNING];
                $available = $job[GearmanHost::WORKER_AVAILABLE];

                if (!isset($this->accumaltiveJobs[$jobName])) {
                    $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_TOTAL]     = 0;
                    $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_RUNNING]   = 0;
                    $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_AVAILABLE] = 0;
                }

                $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_TOTAL] += $total;
                $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_RUNNING] += $running;
                $this->accumaltiveJobs[$jobName][GearmanHost::WORKER_AVAILABLE] = max($this->accumaltiveJobs[$jobName][GearmanHost::WORKER_AVAILABLE],
                    $available);
            }

            foreach ($serverWorkers as $type => $worker) {
                $available = $worker[GearmanHost::WORKER_TOTAL];
                $running   = $worker[GearmanHost::WORKER_RUNNING];
                $free      = $worker[GearmanHost::WORKER_AVAILABLE];
                $queued    = $worker[GearmanHost::WORKER_QUEUED];

                if (!isset($this->accumaltiveWorkers[$type])) {
                    $this->accumaltiveWorkers[$type][GearmanHost::WORKER_TOTAL]     = 0;
                    $this->accumaltiveWorkers[$type][GearmanHost::WORKER_RUNNING]   = 0;
                    $this->accumaltiveWorkers[$type][GearmanHost::WORKER_AVAILABLE] = 0;
                    $this->accumaltiveWorkers[$type][GearmanHost::WORKER_QUEUED]    = 0;
                }

                $this->accumaltiveWorkers[$type][GearmanHost::WORKER_TOTAL] = max($available,
                    $this->accumaltiveWorkers[$type][GearmanHost::WORKER_TOTAL]);
                $this->accumaltiveWorkers[$type][GearmanHost::WORKER_RUNNING] += $running;
                $this->accumaltiveWorkers[$type][GearmanHost::WORKER_QUEUED] += $queued;
            }

            foreach ($this->accumaltiveWorkers as $type => $worker) {
                $this->accumaltiveWorkers[$type][GearmanHost::WORKER_AVAILABLE] = ($worker[GearmanHost::WORKER_TOTAL] - $worker[GearmanHost::WORKER_RUNNING]);
            }
        }

    }
}
