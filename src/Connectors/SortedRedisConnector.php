<?php

namespace Kevinsimard\LaravelSortedQueue\Connectors;

use Illuminate\Queue\Connectors\RedisConnector;
use Illuminate\Support\Arr;
use Kevinsimard\LaravelSortedQueue\SortedRedisQueue;

class SortedRedisConnector extends RedisConnector
{
    /**
     * {@inheritdoc}
     */
    public function connect(array $config)
    {
        $queue = new SortedRedisQueue(
            $this->redis, $config['queue'], Arr::get($config, 'connection', $this->connection)
        );

        $queue->setExpire(Arr::get($config, 'expire', 60));

        return $queue;
    }
}
