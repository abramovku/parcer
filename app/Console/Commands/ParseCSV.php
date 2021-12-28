<?php

namespace App\Console\Commands;

use App\Jobs\parseBatchCsv;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Storage;

class ParseCSV extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'parce:csv {filename} {batch?}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'parse csv file, path is mandatory param';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * @throws \Illuminate\Contracts\Filesystem\FileNotFoundException
     */
    public function handle()
    {
        $filename = $this->argument('filename');
        $batch = !empty($this->argument('batch')) ? (int) $this->argument('batch') : 1000;

        $path = 'app/' . $filename.'.csv';

        $file = fopen(storage_path($path), "r");

        $lineNumber = 1;
        $data = [];
        $data['path'] = $path;

        while (fgets($file) !== false) {
            if ($lineNumber === 1 || ($lineNumber % $batch) === 0) {
                $data['batch'] = $batch;

                //shift first line
                if ($lineNumber === 1) {
                    $data['batch'] = $batch - 1;
                }

                $data['offset'] = ftell($file);
                dispatch(new parseBatchCsv($data));
                $lineNumber++;
                continue;
            }

            $lineNumber++;
        }

        fclose($file);
    }
}
