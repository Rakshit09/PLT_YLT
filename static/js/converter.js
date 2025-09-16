$(document).ready(function() {
    let currentResultData = null;

    // SQL Form Submission
    $('#sqlForm').on('submit', function(e) {
        e.preventDefault();
        
        const data = {
            server: $('#sqlServer').val(),
            database: $('#sqlDatabase').val(),
            table: $('#tableName').val(),
            analysis_no: $('#analysisNo').val(),
            perspective: $('#perspective').val()
        };
        
        $('#loadingSpinner').show();
        $('#resultsSection').hide();
        
        $.ajax({
            url: '/convert_sql',
            type: 'POST',
            data: JSON.stringify(data),
            contentType: 'application/json',
            success: function(response) {
                $('#loadingSpinner').hide();
                displayResults(response);
            },
            error: function(xhr) {
                $('#loadingSpinner').hide();
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'An error occurred';
                alert('Error: ' + error);
            }
        });
    });
    
    // CSV Form Submission
    $('#csvForm').on('submit', function(e) {
        e.preventDefault();
        
        const fileInput = $('#csvFile')[0];
        if (fileInput.files.length === 0) {
            alert('Please select a CSV file');
            return;
        }
        
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        
        $('#loadingSpinner').show();
        $('#resultsSection').hide();
        
        $.ajax({
            url: '/convert_csv',
            type: 'POST',
            data: formData,
            processData: false,
            contentType: false,
            success: function(response) {
                $('#loadingSpinner').hide();
                displayResults(response);
            },
            error: function(xhr) {
                $('#loadingSpinner').hide();
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'An error occurred';
                alert('Error: ' + error);
            }
        });
    });
    
    // Display Results
    function displayResults(result) {
        currentResultData = result;
        
        const html = `
            <div class="result-stats">
                <div class="stat-item">
                    <span class="stat-label">Output File:</span>
                    <span class="stat-value">${result.filename}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Total Rows:</span>
                    <span class="stat-value">${result.rows.toLocaleString()}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">AAL (Average Annual Loss):</span>
                    <span class="stat-value">${result.aal.toFixed(2)}</span>
                </div>
            </div>
            
            <div class="download-section">
                <h5 class="mb-3">Your YLT file is ready!</h5>
                <button class="btn btn-success download-btn" onclick="downloadFile()">
                    <i class="fas fa-download"></i> Download YLT (IFM Format)
                </button>
                <br>
                <small class="text-muted mt-2 d-block">
                    File format: IFM-compatible CSV with escape-delay header
                </small>
            </div>
        `;
        
        $('#resultsContent').html(html);
        $('#resultsSection').show();
    }
});

// Download File Function
function downloadFile() {
    if (!currentResultData) {
        alert('No file to download');
        return;
    }
    
    // Create blob and download
    const blob = new Blob([currentResultData.data], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', currentResultData.filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// Store current result data
let currentResultData = null;