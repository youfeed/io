<?php
/**
 * @micatam
 * 2023-03-28
 * webman redis-queue
 */
namespace Webman\RedisQueue;


/**
 * Interface Consumer
 * @package Webman\RedisQueue
 */
interface Consumer
{
    public function consume($data);
}