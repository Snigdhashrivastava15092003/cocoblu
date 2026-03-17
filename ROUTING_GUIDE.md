# URL Routing Guide for Retail Agent Application

## Overview
The Retail Agent application now supports URL-based routing using query parameters to provide different views for login and main application pages.

## URL Structure

### Login Page
- **URL**: `http://localhost:8005/retail-agent?page=login`
- **Purpose**: Shows the Microsoft authentication login screen
- **Access**: Available to unauthenticated users

### Home/Dashboard Page  
- **URL**: `http://localhost:8005/retail-agent?page=home`
- **Purpose**: Shows the main application dashboard with product comparison tools
- **Access**: Requires Microsoft authentication

### Base URL
- **URL**: `http://localhost:8005/retail-agent`
- **Purpose**: Redirects based on authentication status
- **Access**: Auto-redirects to appropriate page

## Routing Behavior

### Automatic Redirects
1. **Unauthenticated user accessing `?page=home`** → Redirected to `?page=login`
2. **Authenticated user accessing `?page=login`** → Redirected to `?page=home`
3. **Successful login** → Automatically redirected to `?page=home`
4. **Logout** → Redirected to `?page=login`
5. **Base URL access** → Redirected based on authentication status

### Direct URL Access
- Users can directly navigate to either URL with query parameters
- The application will handle authentication checks and redirect as needed
- Browser URL will update to reflect the current page state

## Implementation Details

### Route Detection
- Routes are detected from URL query parameters (`?page=login` or `?page=home`)
- Fallback logic based on authentication state for base URL access
- JavaScript updates browser title and URL for better user experience

### URL Updates
- Browser URL is updated using JavaScript `history.replaceState()`
- No page reload required for URL changes
- Maintains proper browser history and titles

## Testing the Routes

1. **Test Base URL**:
   - Navigate to `http://localhost:8005/retail-agent`
   - Verify it redirects based on authentication status

2. **Test Login Flow**:
   - Navigate to `http://localhost:8005/retail-agent?page=login`
   - Complete Microsoft authentication
   - Verify redirect to `?page=home`

3. **Test Protected Route**:
   - While logged out, try to access `http://localhost:8005/retail-agent?page=home`
   - Verify redirect to login page

4. **Test Logout**:
   - While logged in, click logout
   - Verify redirect to login page with correct URL

## Configuration
- No additional configuration required
- Uses existing Microsoft authentication settings
- Compatible with current Docker setup
- Works with Streamlit's native URL handling