<?php

namespace App\Console\Commands;

use App\Jobs\parseBatchCsv;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\LazyCollection;

class ParseCSV2 extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'parce:csv2 {filename} {batch?}';

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
        $batch = !empty($this->argument('batch')) ? (int) $this->argument('batch') : 100;

        $path = 'app/' . $filename.'.csv';

        LazyCollection::make(function () use ($path) {
            $handle = fopen(storage_path($path), "r");
            while (($line = fgets($handle)) !== false) {
                yield $line;
            }
        })->chunk($batch)->each(function ($rows) {
            foreach($rows as $key => $row) {
                if($key === 0){
                    continue;
                }

                $this->processLine($row);
            }
        });
    }

    protected function processLine(string $data): void
    {
        $row = str_getcsv($data);
        $line = [
            'full_name' => $row[1] . ' ' . $row[2],
            'name' => $row[1],
            'surname' => $row[2],
            'email' => $row[3]
        ];

        DB::table('parsing')->insert($line);
    }
}
