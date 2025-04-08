import luigi
import logging
import wget
import tarfile
import gzip
import shutil
import pandas as pd
from io import StringIO
from pathlib import Path
from typing import Any, Dict

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DownloadDataset(luigi.Task):
    """Task to download dataset from NCBI GEO repository."""
    
    dataset_id = luigi.Parameter()

    def output(self) -> luigi.LocalTarget:
        """Returns output target for raw data."""
        path = Path("data/raw") / f"{self.dataset_id}_RAW.tar"
        return luigi.LocalTarget(str(path))

    def run(self) -> None:
        """Downloads the dataset archive."""
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        url = (
            f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/"
            f"{self.dataset_id[:6]}nnn/{self.dataset_id}/suppl/"
            f"{self.dataset_id}_RAW.tar"
        )

        try:
            logger.info(f"Downloading dataset {self.dataset_id} from {url}")
            wget.download(url, str(output_path))
        except Exception as e:
            logger.error(f"Failed to download dataset: {str(e)}")
            raise luigi.TaskFailed("Dataset download failed")

class ExtractMainArchive(luigi.Task):
    """Task to extract main dataset archive."""
    
    dataset_id = luigi.Parameter()

    def requires(self) -> DownloadDataset:
        return DownloadDataset(self.dataset_id)

    def output(self) -> luigi.LocalTarget:
        """Returns completion flag for extraction."""
        path = Path("data/extracted") / self.dataset_id / "_unpacked"
        return luigi.LocalTarget(str(path))

    def run(self) -> None:
        """Extracts the main archive."""
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with tarfile.open(self.input().path, 'r') as tar:
                tar.extractall(path=output_path.parent)
            
            with self.output().open('w') as f:
                f.write('done')
        except (tarfile.TarError, OSError) as e:
            logger.error(f"Archive extraction failed: {str(e)}")
            raise luigi.TaskFailed("Extraction error")

class ProcessFiles(luigi.Task):
    """Task to process extracted files and create structured data."""
    
    dataset_id = luigi.Parameter()

    def requires(self) -> ExtractMainArchive:
        return ExtractMainArchive(self.dataset_id)

    def output(self) -> luigi.LocalTarget:
        """Returns completion flag for processing."""
        path = Path("data/processed") / self.dataset_id / "_processed"
        return luigi.LocalTarget(str(path))

    def run(self) -> None:
        """Processes GZ files and splits into structured TSV files."""
        input_dir = Path("data/extracted") / self.dataset_id
        output_dir = Path("data/processed") / self.dataset_id
        output_dir.mkdir(parents=True, exist_ok=True)

        for gz_file in input_dir.glob("*.gz"):
            file_stem = gz_file.stem
            file_dir = output_dir / file_stem
            file_dir.mkdir(exist_ok=True)

            # Decompress GZ file
            txt_path = file_dir / f"{file_stem}.txt"
            try:
                with gzip.open(gz_file, 'rb') as f_in, open(txt_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            except (gzip.BadGzipFile, OSError) as e:
                logger.error(f"Failed to decompress {gz_file}: {str(e)}")
                continue

            # Process text file
            tables_dir = file_dir / "tables"
            tables_dir.mkdir(exist_ok=True)
            self._process_text_file(txt_path, tables_dir)

        with self.output().open('w') as f:
            f.write('done')

    def _process_text_file(self, txt_path: Path, tables_dir: Path) -> None:
        """Processes individual text file into structured tables."""
        dfs: Dict[str, pd.DataFrame] = {}
        current_section = None
        buffer = StringIO()

        with open(txt_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('['):
                    self._finalize_section(dfs, current_section, buffer)
                    current_section = line.strip('[]')
                    buffer = StringIO()
                    continue
                if current_section:
                    buffer.write(line + '\n')
            
            # Process final section
            self._finalize_section(dfs, current_section, buffer)

        # Save tables
        for section, df in dfs.items():
            output_path = tables_dir / f"{section}.tsv"
            df.to_csv(output_path, sep='\t', index=False)

    def _finalize_section(self, 
                         dfs: Dict[str, pd.DataFrame], 
                         section: str, 
                         buffer: StringIO) -> None:
        """Finalizes processing of a section."""
        if not section or buffer.tell() == 0:
            return

        buffer.seek(0)
        try:
            header = None if section == "Heading" else "infer"
            dfs[section] = pd.read_csv(buffer, sep='\t', header=header)
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty section {section} in file")

class TrimProbes(luigi.Task):
    """Task to trim unnecessary columns from Probes tables."""
    
    dataset_id = luigi.Parameter()

    def requires(self) -> ProcessFiles:
        return ProcessFiles(self.dataset_id)

    def output(self) -> luigi.LocalTarget:
        """Returns completion flag for trimming."""
        path = Path("data/trimmed") / self.dataset_id / "_trimmed"
        return luigi.LocalTarget(str(path))

    def run(self) -> None:
        """Trims columns from Probes tables."""
        processed_dir = Path("data/processed") / self.dataset_id
        output_dir = Path("data/trimmed") / self.dataset_id

        for probes_path in processed_dir.glob("**/Probes.tsv"):
            relative_path = probes_path.relative_to(processed_dir).parent
            trimmed_dir = output_dir / relative_path
            trimmed_dir.mkdir(parents=True, exist_ok=True)
            
            self._process_probes_file(probes_path, trimmed_dir)

        with self.output().open('w') as f:
            f.write('done')

    def _process_probes_file(self, input_path: Path, output_dir: Path) -> None:
        """Processes individual Probes file."""
        columns_to_drop = [
            'Definition', 'Ontology_Component', 'Ontology_Process',
            'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'
        ]

        try:
            df = pd.read_csv(input_path, sep='\t')
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            df_trimmed = df.drop(columns=existing_columns, errors='ignore')
            output_path = output_dir / "Probes_trimmed.tsv"
            df_trimmed.to_csv(output_path, sep='\t', index=False)
        except Exception as e:
            logger.error(f"Failed to process {input_path}: {str(e)}")

class Cleanup(luigi.Task):
    """Task to clean up intermediate files."""
    
    dataset_id = luigi.Parameter()

    def requires(self) -> TrimProbes:
        return TrimProbes(self.dataset_id)

    def output(self) -> luigi.LocalTarget:
        """Returns completion flag for cleanup."""
        path = Path("data/cleanup") / self.dataset_id / "_cleanup"
        return luigi.LocalTarget(str(path))

    def run(self) -> None:
        """Cleans up intermediate files."""
        processed_dir = Path("data/processed") / self.dataset_id
        
        for txt_file in processed_dir.glob("**/*.txt"):
            try:
                txt_file.unlink()
            except FileNotFoundError:
                continue

        # Remove empty directories
        for root_dir in processed_dir.glob("**/*"):
            if root_dir.is_dir() and not any(root_dir.iterdir()):
                try:
                    root_dir.rmdir()
                except OSError:
                    pass

        with self.output().open('w') as f:
            f.write('done')

class FullPipeline(luigi.WrapperTask):
    """Main pipeline wrapper task."""
    
    dataset_id = luigi.Parameter()

    def requires(self) -> Cleanup:
        return Cleanup(self.dataset_id)

if __name__ == '__main__':
    luigi.run()