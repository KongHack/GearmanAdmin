<?php
namespace GCWorld\GearmanAdmin;

use Exception;

/**
 * Class GearmanClusterAdmin
 * @package GCWorld\GearmanAdmin
 */
class GearmanClusterAdmin
{
    private $cumulativeJobs    = array();
    private $cumulativeWorkers = array();

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

    public function getCumulativeJobs()
    {
        return $this->cumulativeJobs;
    }

    public function getCumulativeWorkers()
    {
        return $this->cumulativeWorkers;
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
        // Run on all gearman servers and collect data and accumulate it
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

                if (!isset($this->cumulativeJobs[$jobName])) {
                    $this->cumulativeJobs[$jobName][GearmanHost::WORKER_TOTAL]     = 0;
                    $this->cumulativeJobs[$jobName][GearmanHost::WORKER_RUNNING]   = 0;
                    $this->cumulativeJobs[$jobName][GearmanHost::WORKER_AVAILABLE] = 0;
                }

                $this->cumulativeJobs[$jobName][GearmanHost::WORKER_TOTAL] += $total;
                $this->cumulativeJobs[$jobName][GearmanHost::WORKER_RUNNING] += $running;
                $this->cumulativeJobs[$jobName][GearmanHost::WORKER_AVAILABLE] = max($this->cumulativeJobs[$jobName][GearmanHost::WORKER_AVAILABLE],
                    $available);
            }

            foreach ($serverWorkers as $type => $worker) {
                $available = $worker[GearmanHost::WORKER_TOTAL];
                $running   = $worker[GearmanHost::WORKER_RUNNING];
                $free      = $worker[GearmanHost::WORKER_AVAILABLE];
                $queued    = $worker[GearmanHost::WORKER_QUEUED];

                if (!isset($this->cumulativeWorkers[$type])) {
                    $this->cumulativeWorkers[$type][GearmanHost::WORKER_TOTAL]     = 0;
                    $this->cumulativeWorkers[$type][GearmanHost::WORKER_RUNNING]   = 0;
                    $this->cumulativeWorkers[$type][GearmanHost::WORKER_AVAILABLE] = 0;
                    $this->cumulativeWorkers[$type][GearmanHost::WORKER_QUEUED]    = 0;
                }

                $this->cumulativeWorkers[$type][GearmanHost::WORKER_TOTAL] = max($available,
                    $this->cumulativeWorkers[$type][GearmanHost::WORKER_TOTAL]);
                $this->cumulativeWorkers[$type][GearmanHost::WORKER_RUNNING] += $running;
                $this->cumulativeWorkers[$type][GearmanHost::WORKER_QUEUED] += $queued;
            }

            foreach ($this->cumulativeWorkers as $type => $worker) {
                $this->cumulativeWorkers[$type][GearmanHost::WORKER_AVAILABLE] = ($worker[GearmanHost::WORKER_TOTAL] - $worker[GearmanHost::WORKER_RUNNING]);
            }
        }

    }
}
