<?php

namespace Kevinsimard\LaravelSortedQueue;

use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Queue\RedisQueue;

class SortedRedisQueue extends RedisQueue
{
    /**
     * {@inheritdoc}
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $id = parent::pushRaw($payload, $queue, $options);

        $this->addQueueIfNotLocked($queue ?: $this->default);

        return $id;
    }

    /**
     * {@inheritdoc}
     */
    public function pop($queue = null)
    {
        $queue = $this->popQueue();

        if (! is_null($queue)) {
            return $this->popJob($queue);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function migrateExpiredJobs($from, $to)
    {
        parent::migrateExpiredJobs($from, $to);

        $this->addQueueIfNotLocked(substr($to, 7));
    }

    /**
     * @param  string  $queue
     * @return bool
     */
    public function lockQueue($queue)
    {
        $key = "sortedqueue:$queue:reserved";

        if ($status = $this->getConnection()->setnx($key, $this->getTime())) {
            $this->getConnection()->expire($key, $this->expire);
        }

        return (bool) $status;
    }

    /**
     * @param  string  $queue
     * @param  int  $seconds
     * @return bool
     */
    public function delayQueue($queue, $seconds)
    {
        $key = "sortedqueue:$queue:delayed";

        if ($status = $this->getConnection()->setnx($key, $this->getTime())) {
            $this->getConnection()->expire($key, $seconds);
        }

        $this->getConnection()->zadd(
            "sortedqueue:delayed", $this->getTime() + $seconds, $queue
        );

        return (bool) $status;
    }

    /**
     * @param  string  $queue
     * @return bool
     */
    public function unlockQueue($queue)
    {
        $deleted = $this->getConnection()->del("sortedqueue:$queue:reserved");

        $this->addQueueIfNotLocked($queue);

        return (bool) $deleted;
    }

    /**
     * @return void
     */
    public function migrateDelayedQueues()
    {
        $queues = $this->getConnection()->zrangebyscore(
            $key = "sortedqueue:delayed", "-inf", $time = $this->getTime()
        );

        if (count($queues) > 0) {
            $this->getConnection()->zremrangebyscore($key, "-inf", $time);

            foreach ($queues as $queue) {
                $this->addQueueIfNotLocked($queue);
            }
        }
    }

    /**
     * @return void
     */
    public function migrateExpiredQueues()
    {
        $cursor = 0;

        do {
            list($cursor, $queues) = $this->getConnection()->scan(
                $cursor, "MATCH", "queues:*:*"
            );

            foreach ($queues as $from) {
                if (ends_with($from, ":delayed")) {
                    $to = substr($from, 0, -8);
                } else if (ends_with($from, ":reserved")) {
                    $to = substr($from, 0, -9);
                } else {
                    continue;
                }

                $this->migrateExpiredJobs($from, $to);
            }
        } while ((int) $cursor !== 0);
    }

    /**
     * @return string|null
     */
    protected function popQueue()
    {
        $queue = $this->getConnection()->spop("sortedqueue:queues");

        if (! is_null($queue)) {
            if (! $this->lockQueue($queue)) {
                return $this->popQueue();
            }

            return $queue;
        }
    }

    /**
     * @param  string  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    protected function popJob($queue)
    {
       $job = $this->getConnection()->lpop($this->getQueue($queue));

        if (! is_null($job)) {
            $this->getConnection()->zadd(
                $this->getQueue($queue).":reserved", $this->getTime() + $this->expire, $job
            );

            return new RedisJob($this->container, $this, $job, $queue);
        }
    }

    /**
     * @param  string  $queue
     * @return void
     */
    protected function addQueueIfNotLocked($queue)
    {
        if (! $this->isQueueLocked($queue) && ! $this->isQueueEmpty($queue)) {
            $this->getConnection()->sadd("sortedqueue:queues", $queue);
        }
    }

    /**
     * @param  string  $queue
     * @return bool
     */
    protected function isQueueLocked($queue)
    {
        return $this->getConnection()->exists(
            "sortedqueue:$queue:reserved",
            "sortedqueue:$queue:delayed"
        ) > 0;
    }

    /**
     * @param  string  $queue
     * @return bool
     */
    protected function isQueueEmpty($queue)
    {
        return $this->getConnection()->llen($this->getQueue($queue)) === 0;
    }
}
