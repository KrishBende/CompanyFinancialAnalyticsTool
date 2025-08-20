// Enhanced UI interactions for the financial analytics web app

document.addEventListener('DOMContentLoaded', function() {
    // Handle form submission with loading indicator
    const form = document.querySelector('form');
    const submitBtn = document.getElementById('submit-btn');
    const originalBtnText = submitBtn ? submitBtn.innerHTML : 'Analyze Data';
    
    if (form && submitBtn) {
        form.addEventListener('submit', function(e) {
            // Show loading state
            submitBtn.innerHTML = '<span class="spinner">⏳</span> Analyzing...';
            submitBtn.disabled = true;
        });
    }
    
    // Example query filling
    const exampleCards = document.querySelectorAll('.example-card');
    const exampleQueries = [
        'Show me total sales by category',
        'Plot the trend of profit over time',
        'What is the average discount by segment?',
        'Predict sales for next year'
    ];
    
    exampleCards.forEach((card, index) => {
        // Set the query text
        const queryText = exampleQueries[index];
        if (queryText) {
            card.querySelector('p').textContent = queryText;
        }
        
        // Add click event
        card.addEventListener('click', function() {
            const input = document.getElementById('query-input');
            if (input) {
                input.value = queryText;
                input.focus();
                
                // Show notification
                showNotification('Query loaded! Press Enter or click Analyze Data', 'info');
            }
        });
    });
    
    // Add keyboard shortcuts
    document.addEventListener('keydown', function(e) {
        // Focus search box on '/'
        if (e.key === '/' && document.activeElement.tagName !== 'INPUT') {
            e.preventDefault();
            const input = document.getElementById('query-input');
            if (input) {
                input.focus();
                showNotification('Type your query and press Enter', 'info');
            }
        }
    });
    
    // Smooth scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth'
                });
            }
        });
    });
});

// Function to show a notification
function showNotification(message, type = 'info') {
    // Remove any existing notifications
    const existingNotifications = document.querySelectorAll('.notification');
    existingNotifications.forEach(notification => notification.remove());
    
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    
    // Add to body
    document.body.appendChild(notification);
    
    // Remove after delay
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 3000);
}

// Function to copy text to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showNotification('Copied to clipboard!', 'success');
    }).catch(err => {
        showNotification('Failed to copy text', 'error');
        console.error('Failed to copy: ', err);
    });
}

// Function to fill query input
function fillQuery(query) {
    const input = document.getElementById('query-input');
    if (input) {
        input.value = query;
        input.focus();
        showNotification('Query loaded! Press Enter or click Analyze Data', 'info');
    }
}