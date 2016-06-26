<?php

namespace Kevinsimard\LaravelSortedQueue;

use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\QueueServiceProvider;
use Kevinsimard\LaravelSortedQueue\Connectors\SortedRedisConnector;
use Kevinsimard\LaravelSortedQueue\SortedRedisQueue;

class SortedQueueServiceProvider extends QueueServiceProvider
{
    /**
     * {@inheritdoc}
     */
    public function register()
    {
        parent::register();

        $this->registerEvents();
    }

    /**
     * {@inheritdoc}
     */
    public function registerConnectors($manager)
    {
        parent::registerConnectors($manager);

        $this->registerSortedRedisConnector($manager);
    }

    /**
     * @return void
     */
    protected function registerEvents()
    {
        $callback = function ($event) {
            if (method_exists($event->job, 'getRedisQueue')) {
                $redisQueue = $event->job->getRedisQueue();

                if ($redisQueue instanceof SortedRedisQueue) {
                    $redisQueue->unlockQueue($event->job->getQueue());
                }
            }
        };

        $this->app['events']->listen(JobFailed::class, $callback);
        $this->app['events']->listen(JobProcessed::class, $callback);
        $this->app['events']->listen(JobExceptionOccurred::class, $callback);
    }

    /**
     * @param  \Illuminate\Queue\QueueManager  $manager
     * @return void
     */
    protected function registerSortedRedisConnector($manager)
    {
        $app = $this->app;

        $manager->addConnector('sorted-redis', function () use ($app) {
            return new SortedRedisConnector($app['redis']);
        });
    }
}
