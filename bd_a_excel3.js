const Papa = require('papaparse');
const XLSX = require('xlsx');
const fs = require('fs').promises;

// Directorio que contiene archivos CSV
const directorioCSV = 'c:/Proyectos/Informes/prueba/';

// Crear un nuevo libro de trabajo Excel
const wb = XLSX.utils.book_new();

// Función principal que procesa el directorio y genera el archivo Excel
async function procesarDirectorio() {
  try {
    // Obtener la lista de archivos en el directorio
    const archivosCSV = await fs.readdir(directorioCSV);

    // Función para procesar cada archivo CSV
    const procesarArchivo = async (archivo) => {
      // Omitir archivos que comienzan por "regis"
      if (archivo.toLowerCase().startsWith('regis')) {
        console.log(`Archivo omitido: ${archivo}`);
        return;
      }

      // Construir la ruta completa al archivo CSV
      const rutaCSV = `${directorioCSV}/${archivo}`;
      
      // Leer el contenido del archivo CSV
      const csvData = await fs.readFile(rutaCSV, 'utf8');
      
      // Utilizar PapaParse para analizar el contenido CSV
      const result = await new Promise((resolve) => {
        Papa.parse(csvData, {
          header: true,
          complete: (result) => resolve(result),
        });
      });

      // Generar un nombre único para la hoja de trabajo en caso de duplicados
      let sheetName = limpiarNombreHoja(archivo.replace('.CSV', ''));
      let sheetIndex = 1;

      while (wb.Sheets[sheetName]) {
        sheetIndex++;
        sheetName = limpiarNombreHoja(archivo.replace('.CSV', '')) + '_' + sheetIndex;
      }

      // Convertir los datos analizados a una hoja de trabajo Excel
      const ws = XLSX.utils.json_to_sheet(result.data.map(rellenarCamposVacios), {
        header: Object.keys(result.data[0]),
      });

      // Agregar la hoja de trabajo al libro de trabajo Excel
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    };

    // Crear un array de promesas para el procesamiento paralelo de archivos
    const promesasProcesamiento = archivosCSV.map(procesarArchivo);

    // Esperar a que todas las promesas se resuelvan
    await Promise.all(promesasProcesamiento);

    // Escribir el libro de trabajo Excel en un archivo
    XLSX.writeFile(wb, 'c:/Proyectos/Informes/prueba/salida.xlsx');
    console.log('Proceso completado.');
  } catch (error) {
    // Manejar errores durante el procesamiento
    console.error('Error durante el procesamiento:', error);
  }
}

// Función para limpiar el nombre de la hoja y asegurar que no exceda los 31 caracteres
function limpiarNombreHoja(nombre) {
  return nombre.substring(0, 31).trim();
}

// Función para rellenar campos vacíos en un objeto de datos con ceros
function rellenarCamposVacios(obj) {
  const newObj = { ...obj }; // Crear un nuevo objeto para no modificar el original
  Object.keys(newObj).forEach((key) => {
    if (newObj[key] === null || newObj[key] === undefined || newObj[key] === '') {
      newObj[key] = 0;
    }
  });
  return newObj;
}

// Iniciar el procesamiento del directorio al ejecutar el script
procesarDirectorio();