const csv = require('csv-parser');
const XLSX = require('xlsx');
const fs = require('fs').promises;

const directorioCSV = 'c:/Python1/prueba/';
const wb = XLSX.utils.book_new();

async function procesarDirectorio() {
  try {
    const archivosCSV = await fs.readdir(directorioCSV);

    const procesarArchivo = async (archivo) => {
      const rutaCSV = `${directorioCSV}/${archivo}`;
      const csvStream = fs.createReadStream(rutaCSV).pipe(csv());

      let sheetName = limpiarNombreHoja(archivo.replace('.CSV', ''));
      let sheetIndex = 1;

      while (wb.Sheets[sheetName]) {
        sheetIndex++;
        sheetName = limpiarNombreHoja(archivo.replace('.CSV', '')) + '_' + sheetIndex;
      }

      const data = await new Promise((resolve, reject) => {
        const rows = [];
        csvStream
          .on('data', (row) => rows.push(row))
          .on('end', () => resolve(rows))
          .on('error', (error) => reject(error));
      });

      const ws = XLSX.utils.json_to_sheet(data.map(rellenarCamposVacios), {
        header: Object.keys(data[0]),
      });

      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    };

    const promesasProcesamiento = archivosCSV.map(procesarArchivo);

    await Promise.all(promesasProcesamiento);

    if (wb.SheetNames.length > 0) {
      XLSX.writeFile(wb, 'c:/Python1/prueba/salida.xlsx');
      console.log('Proceso completado.');
    } else {
      console.log('No se generaron hojas de trabajo.');
    }
  } catch (error) {
    console.error('Error durante el procesamiento:', error);
  }
}

function limpiarNombreHoja(nombre) {
  return nombre.substring(0, 31).trim();
}

function rellenarCamposVacios(obj) {
  const newObj = { ...obj };
  Object.keys(newObj).forEach((key) => {
    if (newObj[key] === null || newObj[key] === undefined || newObj[key] === '') {
      newObj[key] = 0;
    }
  });
  return newObj;
}

procesarDirectorio();