<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class parseBatchCsv implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $data;
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(array $data)
    {
        $this->data = $data;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        $config = $this->data;

        $path = $config['path'];

        $file = fopen(storage_path($path), "r");

        fseek($file, $config['offset']);
        $lineNumber = 1;

        while (
            ($raw_string = fgets($file)) !== false &&
            $lineNumber <= $config['batch']
        ) {

            $row = str_getcsv($raw_string);
            $this->processData($row);

            $lineNumber++;
        }

        fclose($file);
    }

    protected function processData(array $row)
    {
        $data = [
            'full_name' => $row[1] . ' ' . $row[2],
            'name' => $row[1],
            'surname' => $row[2],
            'email' => $row[3]
        ];

        DB::table('parsing')->insert($data);
    }
}
