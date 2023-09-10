$(function() {
    // Details button handler
    $('.details-button').on("click", function(event) {
        const asteroidDetails = $(this).data('details');
        // alert(JSON.stringify(asteroidDetails)); 
        // $('#detailsModal3713989').modal(options)
        // $('#detailsModal3713989').modal('show');
        const data_title = $(this).data('title');
        $('#detailsModal-title').text('Details for ' + data_title)
        $('#detailsModal-details').text(JSON.stringify(asteroidDetails, undefined, 4))
        $('#detailsModal').modal('show');
    });

    let isAscending = true; // A flag to toggle sorting order

    // Sorting handler
    $('th a[href="?sort=name"]').on('click', function(e) {
        e.preventDefault(); // Prevent the default link behavior

        let rows = $('#main-view tbody tr').toArray(); // Get all table rows as an array

        // Sort the array of rows based on the content of the 'Name' column
        rows.sort((a, b) => {
            let nameA = $(a).find('td:first').text();
            let nameB = $(b).find('td:first').text();

            return isAscending ? nameA.localeCompare(nameB) : nameB.localeCompare(nameA);
        });

        // Attach the sorted rows back to the table
        $('#main-view tbody').append(rows);

        // Toggle the sorting order for the next click
        isAscending = !isAscending;
    });    

    // Set default values for search by date interval
    $('#start-date').val('2015-09-07');
    $('#end-date').val('2015-09-08');
});

